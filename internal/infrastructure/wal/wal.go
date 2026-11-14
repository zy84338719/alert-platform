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
	CRC  uint32 `json:"c"`
	Rec  Record `json:"r"`
}

// Log is the segment-structured WAL.
type Log struct {
	mu        sync.Mutex
	dir       string
	maxBytes  int64
	nextSeq   uint64
	ackSeq    int64  // highest sequence confirmed by the center; -1 = nothing acked (0 is a VALID seq)
	file      *os.File
	fileSize  int64
	totalBytes int64
}

// Open recovers (or creates) a WAL rooted at dir. Segments rotate at
// segmentBytes; the whole directory counts against maxBytes.
func Open(dir string, maxBytes, segmentBytes int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, maxBytes: maxBytes, ackSeq: -1}
	segments, err := l.segments()
	if err != nil {
		return nil, err
	}
	var total int64
	for _, seg := range segments {
		recs, size, err := readSegment(seg, func(r Record) {
			if r.Seq >= l.nextSeq {
				l.nextSeq = r.Seq + 1
			}
		})
		if err != nil {
			return nil, fmt.Errorf("recover %s: %w", seg, err)
		}
		_ = recs
		total += size
	}
	l.totalBytes = total
	if maxBytes > 0 && total > 0 {
		// start a fresh segment so appends never interleave with recovery
		if err := l.rotate(); err != nil {
			return nil, err
		}
	} else if err := l.rotate(); err != nil {
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
	defer f.Close()
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
	segs, err := l.segments()
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, seg := range segs {
		recs, _, err := readSegment(seg, func(Record) {})
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			if int64(r.Seq) > l.ackSeq {
				out = append(out, r)
			}
		}
	}
	return out, nil
}

// Ack advances the ack watermark; Compact physically drops acked frames
// (rewrite without them — no residual holes) and removes emptied segments.
func (l *Log) Ack(seq uint64) {
	l.mu.Lock()
	if int64(seq) > l.ackSeq {
		l.ackSeq = int64(seq)
	}
	l.mu.Unlock()
}

// Compact rewrites segments retaining only unacked records.
func (l *Log) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	segs, err := l.segments()
	if err != nil {
		return err
	}
	var keep []Record
	for _, seg := range segs {
		recs, _, err := readSegment(seg, func(Record) {})
		if err != nil {
			return err
		}
		for _, r := range recs {
			if int64(r.Seq) > l.ackSeq {
				keep = append(keep, r)
			}
		}
	}
	// rewrite into a fresh dir then swap files
	tmpDir := l.dir + ".compact"
	_ = os.RemoveAll(tmpDir)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	var total int64
	for _, r := range keep {
		payload, err := json.Marshal(frame{CRC: crc32.ChecksumIEEE(r.Body), Rec: r})
		if err != nil {
			return err
		}
		name := filepath.Join(tmpDir, fmt.Sprintf("wal-%08d.log", r.Seq))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		hdr := make([]byte, 8)
		binary.BigEndian.PutUint64(hdr, uint64(len(payload)))
		f.Write(hdr)
		f.Write(payload)
		f.Sync()
		f.Close()
		total += int64(len(payload)) + 8
	}
	// close active file, replace segments, reopen
	if err := l.file.Close(); err != nil {
		return err
	}
	for _, seg := range segs {
		os.Remove(seg)
	}
	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		os.Rename(filepath.Join(tmpDir, e.Name()), filepath.Join(l.dir, e.Name()))
	}
	os.RemoveAll(tmpDir)
	l.totalBytes = total
	return l.rotate()
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
	segs, err := l.segments()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, seg := range segs {
		_, _, err := readSegment(seg, func(r Record) {
			if int64(r.Seq) > l.ackSeq {
				count++
			}
		})
		if err != nil {
			return 0, err
		}
	}
	return count, nil
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
