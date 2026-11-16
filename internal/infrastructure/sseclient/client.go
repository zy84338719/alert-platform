// Package sseclient is a standard-library SSE (text/event-stream) client for
// the HUATUO collector (HT-001): ordered event delivery, reconnect with the
// last cursor, heartbeat-timeout detection, and GAP REPORTING — SSE streams
// are NOT replayable, so a detected gap is surfaced as an explicit gap event
// the caller records (docs/04 §3: 无法补齐，应报警并记录缺口).
package sseclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Event is one parsed SSE event.
type Event struct {
	ID             string
	Name           string // event:; "" = default "message"
	Data           string
	Gap            bool // true when this is a synthesized gap marker (not from the wire)
	GapFrom, GapTo string
}

// Handler consumes events. Returning the exported ErrStop sentinel ends Run
// cleanly (nil); any other error ends Run with that error.
type Handler func(Event) error

// ErrStop is the clean-termination sentinel for handlers.
var ErrStop = errors.New("sse: stop")

// Client connects to one SSE endpoint.
type Client struct {
	URL    string
	Header http.Header
	// P0-04: the official HUATUO /v1/events/watch is HTTP POST + JSON body +
	// Bearer token. Method defaults to GET for plain SSE; POST requests send
	// Body with Content-Type: application/json; BearerToken is sent as the
	// Authorization header.
	Method           string
	Body             []byte
	BearerToken      string
	HeartbeatTimeout time.Duration // no data for this long -> reconnect (default 30s)
	// CursorKind: how to detect gaps. "seq" compares numeric event ids.
	GapDetection bool

	mu      sync.Mutex
	lastID  string
	lastSeq int64
}

// New builds a client with defaults.
func New(url string) *Client {
	return &Client{URL: url, HeartbeatTimeout: 30 * time.Second, GapDetection: true}
}

// LastID exposes the cursor (for lease handoff / metrics).
func (c *Client) LastID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastID
}

// Run streams until ctx is done or handler fails permanently. Reconnects use
// Last-Event-ID. Returns ctx.Canceled on shutdown, otherwise the fatal error.
func (c *Client) Run(ctx context.Context, handle Handler) error {
	for {
		if err := c.streamOnce(ctx, handle); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			if isPermanent(handleErr(err)) { // handler asked to stop
				if unWrap(err) == ErrStop {
					return nil
				}
				return unWrap(err)
			}
			// transient: brief pause then reconnect (gap detection follows)
			reconnectTotal.Inc()
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

type handlerError struct{ err error }

func (e *handlerError) Error() string { return "handler: " + e.err.Error() }
func handleErr(err error) error       { return err }
func isPermanent(err error) bool {
	var he *handlerError
	return errors.As(err, &he)
}
func unWrap(err error) error {
	var he *handlerError
	if errors.As(err, &he) {
		return he.err
	}
	return err
}

func (c *Client) streamOnce(ctx context.Context, handle Handler) error {
	method := c.Method
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	if len(c.Body) > 0 {
		body = bytes.NewReader(c.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.URL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if len(c.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	for k, vs := range c.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	c.mu.Lock()
	if c.lastID != "" {
		req.Header.Set("Last-Event-ID", c.lastID)
	}
	c.mu.Unlock()

	client := &http.Client{
		Timeout: 0, // stream lives long; heartbeat timeout governs
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, //nolint:gosec
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sse endpoint returned %d", resp.StatusCode)
	}

	// Heartbeat watchdog (P0-04): driven by LAST ACTIVITY, not a fixed timer —
	// every received line (including ": ping" keepalive comments and events)
	// refreshes the deadline, so an actively streaming connection is never
	// force-disconnected; a silent one is reconnected after the timeout.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// atomic: the scanner goroutine writes, the watchdog reads (P1 race hygiene)
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	go func() {
		// check at 1/4 of the window so the detected stall is <= timeout
		interval := c.HeartbeatTimeout / 4
		if interval < 50*time.Millisecond {
			interval = 50 * time.Millisecond
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-t.C:
				last := time.Unix(0, lastActivity.Load())
				if time.Since(last) > c.HeartbeatTimeout {
					// silent for the whole window: force reconnect by closing the body
					_ = resp.Body.Close()
					return
				}
			}
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var name, data strings.Builder
	pendingID := ""
	flush := func() error {
		if data.Len() == 0 && name.Len() == 0 {
			return nil
		}
		ev := Event{ID: pendingID, Name: name.String(), Data: strings.TrimSuffix(data.String(), "\n")}
		name.Reset()
		data.Reset()
		pendingID = ""
		return ev2(ev, c, handle)
	}
	for scanner.Scan() {
		lastActivity.Store(time.Now().UnixNano())
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return &handlerError{err}
			}
			continue
		}
		// Comment/heartbeat lines (": ping") are keepalives only: lastActivity
		// is refreshed above, nothing to dispatch.
		if strings.HasPrefix(line, ":") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			name.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			data.WriteString("\n")
		case strings.HasPrefix(line, "id:"):
			id := strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			// gap detection on numeric cursors (HT-004): a jump > 1 means
			// events were lost while disconnected — SSE cannot replay them.
			if c.GapDetection {
				if seq, err := strconv.ParseInt(id, 10, 64); err == nil {
					c.mu.Lock()
					prev := c.lastSeq
					c.mu.Unlock()
					if prev > 0 && seq > prev+1 {
						gap := Event{
							Gap: true, Name: "gap",
							GapFrom: strconv.FormatInt(prev+1, 10),
							GapTo:   strconv.FormatInt(seq-1, 10),
							ID:      id,
						}
						if err := handle(gap); err != nil {
							return &handlerError{err}
						}
					}
					c.mu.Lock()
					c.lastSeq = seq
					c.mu.Unlock()
				}
			}
			pendingID = id
			c.mu.Lock()
			c.lastID = id
			c.mu.Unlock()
		}
	}
	return scanner.Err()
}

func ev2(ev Event, c *Client, handle Handler) error {
	if ev.Name == "" {
		ev.Name = "message"
	}
	return handle(ev)
}
