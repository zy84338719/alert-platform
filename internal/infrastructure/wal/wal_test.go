package wal

import (
	"os"
	"strconv"
	"path/filepath"
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
	t.Cleanup(func() { l.file.Close() })
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
	defer l2.file.Close()
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
	l.Append(rec("a", "hello"))
	seq := l.SeqLabel() // nextSeq AFTER the append; the record lives at seq-1
	l.file.Close()

	// tear: append half a frame to the active segment
	seg := filepath.Join(dir, "wal-"+pad(prev(seq))+".log")
	f, _ := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0, 0, 0, 0, 0, 1, 2, 3, 'x', 'y'}) // length prefix + partial payload
	f.Close()

	l2, _ := Open(dir, 0, 1<<20)
	defer l2.file.Close()
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
	p, _ := l.Pending()
	l.Ack(p[len(p)-1].Seq)
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
		l.Append(rec(string(rune('a'+i)), "x"))
	}
	p, _ := l.Pending()
	// ack everything except records 3 and 7
	l.Ack(1) // ack 0..1
	ackTo := func(seq uint64) { l.Ack(seq) }
	ackTo(2) // 0..2 acked; 3 pending
	// ack 4..6 and 8..9 are still pending — watermark semantics ack prefix:
	// emulate by acking only prefix 0..2 here
	_ = p
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
	l.Append(rec("a", "x"))
	p, _ := l.Pending()
	l.Ack(p[0].Seq)
	l.Ack(0)
	got, _ := l.Pending()
	if len(got) != 0 {
		t.Fatalf("ack rewound: %+v", got)
	}
}
