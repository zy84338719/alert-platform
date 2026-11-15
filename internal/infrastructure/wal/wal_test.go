package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// EDGE-001/004 acceptance battery for the WAL.

func newLog(t *testing.T, maxBytes int64) *Log {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, maxBytes, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.file.Close() })
	return l
}

func rec(id, body string) Record { return Record{ID: id, Body: []byte(body)} }

// fsync-before-202 is structural (Append syncs before returning); this test
// pins durability the observable way: every Append that returned nil is
// present after a "crash" (close + reopen = kill -9 equivalent, no graceful
// flush hook exists anyway).
func TestAppendSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := l.Append(rec(string(rune('a'+i%26)), "payload")); err != nil {
			t.Fatal(err)
		}
	}
	// simulate kill -9: the process dies; nothing flushes on close.
	_ = l.file.Close()

	l2, err := Open(dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l2.file.Close() }()
	p, err := l2.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 100 {
		t.Fatalf("recovered %d records after crash, want 100", len(p))
	}
	for i, r := range p {
		if r.Seq != uint64(i) {
			t.Fatalf("order broken at %d: seq %d", i, r.Seq)
		}
	}
}

// A torn trailing frame (power loss mid-write) is dropped, earlier frames
// survive — the torn event was never acked.
func TestTornFrameDropped(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, 0, 1<<20)
	if _, err := l.Append(rec("a", "hello")); err != nil {
		t.Fatalf("append: %v", err)
	}
	seq := l.SeqLabel() // nextSeq AFTER the append; the record lives at seq-1
	_ = l.file.Close()

	// tear: append half a frame to the active segment
	seg := filepath.Join(dir, "wal-"+pad(prev(seq))+".log")
	f, _ := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if _, err := f.Write([]byte{0, 0, 0, 0, 0, 1, 2, 3, 'x', 'y'}); err != nil { // length prefix + partial payload
		t.Fatalf("tear write: %v", err)
	}
	_ = f.Close()

	l2, _ := Open(dir, 0, 1<<20)
	defer func() { _ = l2.file.Close() }()
	p, _ := l2.Pending()
	if len(p) != 1 || p[0].ID != "a" {
		t.Fatalf("recovered %+v, want only the intact record", p)
	}
}

func pad(s string) string {
	for len(s) < 8 {
		s = "0" + s
	}
	return s
}

// prev decrements the decimal sequence label by one.
func prev(s string) string {
	n, _ := strconv.ParseUint(s, 10, 64)
	return strconv.FormatUint(n-1, 10)
}

// Disk-full equivalence: the capacity bound refuses Appends (the edge must
// answer the source with an error, never a silent drop).
func TestCapacityFullFailsClosed(t *testing.T) {
	l := newLog(t, 256)
	for i := 0; ; i++ {
		_, err := l.Append(rec("r", "0123456789"))
		if err == nil {
			if i > 100 {
				t.Fatal("capacity never enforced")
			}
			continue
		}
		if err != ErrFull {
			t.Fatalf("want ErrFull, got %v", err)
		}
		break
	}
	// Acked + compacted space is reclaimed: appends succeed again.
	// (P0-02: per-record acks — confirm the whole prefix, as the uploader does.)
	p, _ := l.Pending()
	for _, r := range p {
		l.Ack(r.Seq)
	}
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(rec("after", "0123456789")); err != nil {
		t.Fatalf("append after compact: %v", err)
	}
}

// Compact leaves no residual holes: only unacked records remain, in order.
func TestCompactNoHoles(t *testing.T) {
	l := newLog(t, 0)
	for i := 0; i < 10; i++ {
		if _, err := l.Append(rec(string(rune('a'+i)), "x")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// P0-02 per-record acks: confirm prefix 0..2 exactly like the uploader.
	l.Ack(0)
	l.Ack(1)
	l.Ack(2)
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}
	got, _ := l.Pending()
	if len(got) != 7 { // seqs 3..9
		t.Fatalf("after compact pending=%d, want 7: %+v", len(got), got)
	}
	if got[0].Seq != 3 {
		t.Fatalf("first pending seq = %d, want 3", got[0].Seq)
	}
}

// Repeated ack of the same sequence never rewinds the watermark.
func TestAckMonotonic(t *testing.T) {
	l := newLog(t, 0)
	if _, err := l.Append(rec("a", "x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	p, _ := l.Pending()
	l.Ack(p[0].Seq)
	l.Ack(0)
	got, _ := l.Pending()
	if len(got) != 0 {
		t.Fatalf("ack rewound: %+v", got)
	}
}

// P0-02 (rh-p0-02) 失败测试①：跳跃 ACK。uploader 对 seq 1 的 HTTP 400 拒单
// 不确认、seq 2 成功确认 —— Ack 是"逐记录确认"，水位只能推进到最长连续确认
// 前缀；旧实现 Ack(2) 直接把水位抬到 2，被拒的 seq 1 连同 seq 0 一起被
// Compact 清掉（断网缓冲告警丢失）。
func TestSkippedAckKeepsRejectedRecord(t *testing.T) {
	l := newLog(t, 0)
	for i := 0; i < 3; i++ {
		if _, err := l.Append(rec(fmt.Sprintf("b%d", i), "payload")); err != nil {
			t.Fatal(err)
		}
	}
	l.Ack(0) // seq 0 accepted
	l.Ack(2) // seq 1 REJECTED (no ack); seq 2 accepted
	got, _ := l.Pending()
	// seq 2 is confirmed above the hole: not re-pended (center already has it)
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("pending after skipped ack = %+v, want [seq 1] only", got)
	}
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}
	got, _ = l.Pending()
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("pending after compact = %+v, want [seq 1] — the rejected record must survive", got)
	}
	// the hole closes only when seq 1 is finally confirmed
	l.Ack(1)
	if got, _ = l.Pending(); len(got) != 0 {
		t.Fatalf("pending after closing ack = %+v, want empty", got)
	}
}

// P0-02 (rh-p0-02) 失败测试②：Compact 崩溃注入 + 重启恢复。Compact 在每个阶段
// 边界以 panic 模拟 kill -9（磁盘状态与真实进程死亡一致：已 fsync 的都在），
// 重启后必须恢复全部未确认记录，任何崩溃窗口零丢失。
func TestCompactCrashWindowsRecover(t *testing.T) {
	for _, window := range []string{"before-rename", "after-rename"} {
		t.Run(window, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, 0, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 10; i++ {
				if _, err := l.Append(rec(fmt.Sprintf("b%d", i), "payload")); err != nil {
					t.Fatal(err)
				}
			}
			// uploader 语义：前缀 0..6 已确认，7..9 未确认
			for s := uint64(0); s <= 6; s++ {
				l.Ack(s)
			}
			switch window {
			case "before-rename":
				l.crashBeforeRename = func() { panic("kill -9 before rename") }
			case "after-rename":
				l.crashAfterRename = func() { panic("kill -9 after rename") }
			}
			func() {
				defer func() { _ = recover() }() // 进程级死亡等价
				_ = l.Compact()
			}()
			// 重启：从崩溃留下的磁盘状态恢复（不用旧实例的任何内存状态）
			l2, err := Open(dir, 0, 1<<20)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = l2.Close() })
			got, err := l2.Pending()
			if err != nil {
				t.Fatal(err)
			}
			// 重启后 ack 水位归零是既有设计（全量重传、中央 receipts 幂等吸收）；
			// 崩溃安全的断言是零丢失：compact 前存在的每条记录（0..9）重启后
			// 仍可读、各出现一次（崩溃窗口的瞬态重复帧被去重）。
			if len(got) != 10 {
				t.Fatalf("recovered %d records after crash-in-compact, want all 10 (zero loss): %+v", len(got), got)
			}
			for i, r := range got {
				if r.Seq != uint64(i) {
					t.Fatalf("recovered seq %d at %d — sequence corrupted", r.Seq, i)
				}
			}
		})
	}
}
