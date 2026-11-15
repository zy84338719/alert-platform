// Package wal implements the Edge write-ahead log (EDGE-001): every accepted
// vendor event is appended and fsynced BEFORE the HTTP 202 leaves the edge,
// so a kill -9 never loses an acknowledged event. Records carry a stable
// transport id — the uploader retransmits the SAME id until the center acks,
// and the center's receipts layer absorbs the duplicates (docs/04 §3).
//
// Format: length-prefixed JSON frames in segment files wal-<seq>.log. A
// trailing partial frame (torn write) is detected on recovery and dropped —
// the event was never acknowledged. Capacity is bounded by maxBytes; a full
// log fails Append with ErrFull so the caller returns an error to the source
// (and raises its own alert) instead of accepting data it cannot buffer.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/example/alert-platform/internal/application/edge"
)

// Errors surfaced by the edge receiver.
var (
	ErrFull = errors.New("wal: capacity limit reached (disk-full equivalent)")
)

// Record is one buffered canonical batch; the application layer owns the
// type (edge.Record) and the WAL persists it verbatim.
type Record = edge.Record

type frame struct {
	CRC uint32 `json:"c"`
	Rec Record `json:"r"`
}

// Log is the segment-structured WAL.
type Log struct {
	mu         sync.Mutex
	dir        string
	maxBytes   int64
	nextSeq    uint64
	ackSeq     int64               // longest CONTIGUOUS confirmed prefix; -1 = nothing confirmed (0 is a VALID seq)
	ackedSet   map[uint64]struct{} // confirmed seqs above the watermark; a hole below blocks advance
	file       *os.File
	fileSize   int64
	totalBytes int64

	// Crash-injection points for tests (nil in production): Compact aborts at
	// each phase boundary, leaving the exact on-disk state a kill -9 would
	// leave, so recovery is proven per window (TestCompactCrashWindowsRecover).
	crashBeforeRename func()
	crashAfterRename  func()
}

// Open recovers (or creates) a WAL rooted at dir. Segments rotate at
// segmentBytes; the whole directory counts against maxBytes.
func Open(dir string, maxBytes, segmentBytes int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// A crash mid-Compact leaves in-progress replacement files behind
	// (".compact-tmp"); they were never renamed in, so they carry no records
	// the segments don't — drop them before recovery.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".compact-tmp") {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
	l := &Log{dir: dir, maxBytes: maxBytes, ackSeq: -1, ackedSet: make(map[uint64]struct{})}
	segments, err := l.segments()
	if err != nil {
		return nil, err
	}
	var total int64
	for _, seg := range segments {
		_, size, err := readSegment(seg, func(r Record) {
			if r.Seq >= l.nextSeq {
				l.nextSeq = r.Seq + 1
			}
		})
		if err != nil {
			return nil, fmt.Errorf("recover %s: %w", seg, err)
		}
		total += size
	}
	l.totalBytes = total
	// start a fresh segment so appends never interleave with recovery
	if err := l.rotate(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) segments() ([]string, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			out = append(out, filepath.Join(l.dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

func readSegment(path string, each func(Record)) ([]Record, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	var out []Record
	var valid int64
	for {
		lenBuf := make([]byte, 8)
		if _, err := io.ReadFull(f, lenBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean EOF, or torn trailing frame: drop it
			}
			return nil, 0, err
		}
		size := binary.BigEndian.Uint64(lenBuf)
		if size == 0 || size > 64<<20 {
			break // garbage after a torn write: stop scanning this segment
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // torn frame: never acked, safe to drop
		}
		var fr frame
		if err := json.Unmarshal(payload, &fr); err != nil {
			break
		}
		if crc32.ChecksumIEEE(fr.Rec.Body) != fr.CRC {
			break // corrupted frame: stop (earlier frames stay valid)
		}
		out = append(out, fr.Rec)
		valid += int64(8 + size)
		each(fr.Rec)
	}
	st, _ := f.Stat()
	return out, st.Size(), nil
}

// Append writes one record and fsyncs BEFORE returning — the caller may only
// answer 202 after this returns nil (EDGE-001 acceptance).
func (l *Log) Append(rec Record) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.maxBytes > 0 && l.totalBytes+int64(len(rec.Body))+64 > l.maxBytes {
		return rec, ErrFull
	}
	rec.Seq = l.nextSeq
	l.nextSeq++
	payload, err := json.Marshal(frame{CRC: crc32.ChecksumIEEE(rec.Body), Rec: rec})
	if err != nil {
		return rec, err
	}
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint64(hdr, uint64(len(payload)))
	if _, err := l.file.Write(hdr); err != nil {
		return rec, err
	}
	if _, err := l.file.Write(payload); err != nil {
		return rec, err
	}
	// fsync BEFORE success is returned — power-loss equivalence (EDGE-001).
	if err := l.file.Sync(); err != nil {
		return rec, err
	}
	l.totalBytes += int64(len(payload)) + 8
	l.fileSize += int64(len(payload)) + 8
	return rec, nil
}

// Pending returns records not yet acknowledged, in sequence order.
func (l *Log) Pending() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unackedLocked()
}

// unackedLocked walks every segment in order and returns the pending records:
// seq neither covered by the confirmed prefix nor individually confirmed.
// (Confirmed-above-a-hole records are NOT re-pended — the center already has
// them; Compact drops them physically.) A crash mid-Compact can transiently
// duplicate frames across the pre/post replacement files — the first
// occurrence in scan order wins. Caller holds l.mu.
func (l *Log) unackedLocked() ([]Record, error) {
	segs, err := l.segments()
	if err != nil {
		return nil, err
	}
	seen := make(map[uint64]struct{})
	var out []Record
	for _, seg := range segs {
		recs, _, err := readSegment(seg, func(Record) {})
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			if l.confirmedLocked(r.Seq) {
				continue
			}
			if _, dup := seen[r.Seq]; dup {
				continue
			}
			seen[r.Seq] = struct{}{}
			out = append(out, r)
		}
	}
	return out, nil
}

// confirmedLocked reports whether a record's delivery is already confirmed
// (contiguous prefix or per-record ack). Caller holds l.mu.
func (l *Log) confirmedLocked(seq uint64) bool {
	if int64(seq) <= l.ackSeq {
		return true
	}
	_, ok := l.ackedSet[seq]
	return ok
}

// Ack confirms ONE record (P0-02: per-record confirmation, not a watermark).
// The watermark advances only across the longest contiguous confirmed prefix:
// a rejected record below a successful one keeps the watermark below itself,
// so Compact can never drop either — the rejected record stays pending for
// retry and the confirmed one stays buffered until the hole closes.
func (l *Log) Ack(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if int64(seq) <= l.ackSeq {
		return
	}
	l.ackedSet[seq] = struct{}{}
	for {
		next := uint64(l.ackSeq + 1)
		if _, ok := l.ackedSet[next]; !ok {
			break
		}
		delete(l.ackedSet, next)
		l.ackSeq = int64(next)
	}
}

// Compact rewrites segments retaining only unacked records (P0-02): the
// replacement segment is written + fsynced + RENAMED IN first, and only then
// are the superseded segments removed — every crash window leaves at least
// one durable copy of every unacked record on disk. Order:
//
//  1. write replacement (".compact-tmp") + fsync file + fsync dir
//  2. close the active segment (its unacked frames are in the replacement)
//  3. rename tmp → wal-<first-keep-seq>.log (atomic; same dir) + fsync dir
//  4. remove superseded segments + fsync dir
//  5. rotate a fresh active segment
//
// A kill -9 between any two steps recovers to the pre-step state (step 1–2)
// or to a superset of the unacked records (step 3–4, transient duplicates
// that unackedLocked dedupes) — never to a state missing unacked data.
func (l *Log) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	keep, err := l.unackedLocked()
	if err != nil {
		return err
	}
	segs, err := l.segments()
	if err != nil {
		return err
	}
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return err
		}
		l.file = nil
	}
	if len(keep) == 0 {
		// everything on disk is confirmed (prefix or per-record): drop it all
		for _, seg := range segs {
			if err := os.Remove(seg); err != nil {
				return err
			}
		}
		if err := syncDir(l.dir); err != nil {
			return err
		}
		l.totalBytes = 0
		l.fileSize = 0
		// confirmed records above a hole were physically dropped: their ack
		// bits are dead weight — clear them so the set stays bounded
		l.ackedSet = make(map[uint64]struct{})
		return l.rotate()
	}
	final := filepath.Join(l.dir, fmt.Sprintf("wal-%08d.log", keep[0].Seq))
	tmp := final + ".compact-tmp"
	var total int64
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	for _, r := range keep {
		payload, err := json.Marshal(frame{CRC: crc32.ChecksumIEEE(r.Body), Rec: r})
		if err != nil {
			_ = f.Close()
			return err
		}
		hdr := make([]byte, 8)
		binary.BigEndian.PutUint64(hdr, uint64(len(payload)))
		if _, err := f.Write(hdr); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(payload); err != nil {
			_ = f.Close()
			return err
		}
		total += int64(len(payload)) + 8
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	if l.crashBeforeRename != nil {
		l.crashBeforeRename()
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	if l.crashAfterRename != nil {
		l.crashAfterRename()
	}
	// the replacement is durable: drop every superseded segment
	for _, seg := range segs {
		if seg == final {
			continue
		}
		if err := os.Remove(seg); err != nil {
			return err
		}
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	l.totalBytes = total
	l.fileSize = 0
	// every kept record is unconfirmed by definition, so all ack bits left in
	// the set refer to records this Compact just dropped — clear them (bounded
	// memory); the watermark only stalls at the hole, which nothing above it
	// depends on anymore
	l.ackedSet = make(map[uint64]struct{})
	return l.rotate()
}

// syncDir fsyncs a directory so rename/unlink are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// Stats reports sizes for the disk-waterline metrics (EDGE-003).
// (pending is computed inline: Pending() also takes the mutex, so calling it
// here would self-deadlock — the exact bug the T-011 drill flushed out.)
func (l *Log) Stats() (bytes int64, pending int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, _ := l.pendingLocked()
	return l.totalBytes, n
}

// pendingLocked counts unacked records; caller holds l.mu.
func (l *Log) pendingLocked() (int, error) {
	recs, err := l.unackedLocked()
	if err != nil {
		return 0, err
	}
	return len(recs), nil
}

// Close releases the active segment (graceful shutdown / tests).
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// rotate starts a new segment file.
func (l *Log) rotate() error {
	name := filepath.Join(l.dir, fmt.Sprintf("wal-%08d.log", l.nextSeq))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if l.file != nil {
		_ = l.file.Close()
	}
	l.file = f
	l.fileSize = 0
	return nil
}

// SeqLabel exposes the sequence for tests.
func (l *Log) SeqLabel() string { return strconv.FormatUint(l.nextSeq, 10) }

// Dir exposes the WAL root (tests/recovery).
func (l *Log) Dir() string { return l.dir }
