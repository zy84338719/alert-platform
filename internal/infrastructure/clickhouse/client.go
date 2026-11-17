// Package clickhouse is the HTTP-only ClickHouse adapter for the transition
// archive (P9-CH-01). It deliberately avoids a driver dependency: insert uses
// JSONEachRow, read uses FORMAT JSON, and tenant/time values travel as bound
// query parameters ({tenant:UUID} etc.) — never string-interpolated.
package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/example/alert-platform/internal/application/archive"
	"github.com/example/alert-platform/internal/application/history"
)

// Client talks to the ClickHouse HTTP interface.
type Client struct {
	base     string
	database string
	user     string
	password string
	hc       *http.Client
}

// New builds a client; base e.g. "http://clickhouse:8123".
func New(base, database, user, password string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"), database: database,
		user: user, password: password,
		hc: &http.Client{Timeout: 15 * time.Second},
	}
}

const chTimeLayout = "2006-01-02 15:04:05.000"

// InsertTransitions writes one batch (atomic per HTTP request).
func (c *Client) InsertTransitions(ctx context.Context, rows []archive.Transition) error {
	if len(rows) == 0 {
		return nil
	}
	var body strings.Builder
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		row := map[string]any{
			"tenant_id":     r.TenantID,
			"transition_id": r.TransitionID,
			"instance_id":   r.InstanceID,
			"from_state":    r.FromState,
			"to_state":      r.ToState,
			"event_id":      r.EventID,
			"transition_at": r.At.UTC().Format(chTimeLayout),
		}
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("clickhouse: encode row: %w", err)
		}
	}
	return c.post(ctx, "INSERT INTO alert_transitions FORMAT JSONEachRow", nil, body.String())
}

// ListTransitionsRange implements history.Store over the archive.
func (c *Client) ListTransitionsRange(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]history.Transition, error) {
	// AUD-013: the DateTime64 parameters carry an EXPLICIT 'UTC' type — a bare
	// DateTime64 is parsed in the server/session timezone and shifts the read
	// window on any non-UTC instance. FINAL hides unmerged ReplacingMergeTree
	// duplicates (at-least-once archive writes).
	q := `SELECT transition_id, instance_id, from_state, to_state, event_id, transition_at
	      FROM alert_transitions FINAL
	      WHERE tenant_id = {tenant:UUID}
	        AND transition_at >= {from:DateTime64(3, 'UTC')}
	        AND transition_at < {to:DateTime64(3, 'UTC')}
	      ORDER BY transition_at DESC
	      LIMIT {lim:UInt32}
	      FORMAT JSON`
	params := map[string]string{
		"param_tenant": tenantID,
		"param_from":   from.UTC().Format(chTimeLayout),
		"param_to":     to.UTC().Format(chTimeLayout),
		"param_lim":    fmt.Sprintf("%d", limit),
	}
	body, err := c.postBody(ctx, q, params)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Data []struct {
			TransitionID string `json:"transition_id"`
			InstanceID   string `json:"instance_id"`
			FromState    string `json:"from_state"`
			ToState      string `json:"to_state"`
			EventID      string `json:"event_id"`
			TransitionAt string `json:"transition_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("clickhouse: decode transitions: %w", err)
	}
	out := make([]history.Transition, 0, len(parsed.Data))
	for _, r := range parsed.Data {
		at, err := parseCHTime(r.TransitionAt)
		if err != nil {
			return nil, err
		}
		out = append(out, history.Transition{
			TransitionID: r.TransitionID, InstanceID: r.InstanceID,
			FromState: r.FromState, ToState: r.ToState, EventID: r.EventID, At: at,
		})
	}
	return out, nil
}

func parseCHTime(raw string) (time.Time, error) {
	for _, layout := range []string{chTimeLayout, "2006-01-02 15:04:05", time.RFC3339Nano} {
		if t, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("clickhouse: unparsable timestamp %q", raw)
}

func (c *Client) post(ctx context.Context, query string, params map[string]string, body string) error {
	_, err := c.postBodyWithQuery(ctx, query, params, body)
	return err
}

func (c *Client) postBody(ctx context.Context, query string, params map[string]string) ([]byte, error) {
	return c.postBodyWithQuery(ctx, query, params, "")
}

func (c *Client) postBodyWithQuery(ctx context.Context, query string, params map[string]string, body string) ([]byte, error) {
	if c.base == "" {
		return nil, fmt.Errorf("clickhouse: base URL not configured")
	}
	q := url.Values{}
	q.Set("query", query)
	// AUD-001（#10）: without wait_end_of_query the HTTP status can be sent
	// BEFORE the statement finished; execution-time errors are then injected
	// into the body while the status stays 200 — the archiver would advance
	// its cursor and silently lose the batch. This makes the status truthful.
	q.Set("wait_end_of_query", "1")
	if c.database != "" {
		q.Set("database", c.database)
	}
	for k, v := range params {
		q.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/?"+q.Encode(), strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: build request: %w", err)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("clickhouse: HTTP %d: %s", resp.StatusCode, snippet(respBody))
		// 4xx means the statement/data was rejected: retrying the identical
		// batch every tick can never succeed (AUD-001 classification).
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, &PermanentError{Err: errors.New(msg)}
		}
		return nil, errors.New(msg)
	}
	// Belt and braces: a 200 body may still carry a ClickHouse exception
	// (implementations/proxies that ignore wait_end_of_query). Never treat
	// such a response as success.
	if looksLikeCHException(respBody) {
		return nil, fmt.Errorf("clickhouse: execution error in 200 body: %s", snippet(respBody))
	}
	return respBody, nil
}

// PermanentError marks a CH rejection that retrying cannot fix (4xx / data
// errors). Callers dead-letter such rows instead of retrying forever.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "clickhouse permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// IsPermanent reports whether err (or its chain) is a PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// looksLikeCHException detects an exception payload in a 200 response body.
// ClickHouse exceptions start with "Code: <n>. DB::Exception:".
func looksLikeCHException(body []byte) bool {
	head := body
	if len(head) > 256 {
		head = head[:256]
	}
	s := string(head)
	return strings.HasPrefix(strings.TrimSpace(s), "Code: ") && strings.Contains(s, "DB::Exception")
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

// chCount decodes ClickHouse JSON counts that may be quoted (the HTTP JSON
// format quotes 64-bit integers by default).
type chCount int64

func (c *chCount) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return err
	}
	*c = chCount(n)
	return nil
}

// CountTransitionsByDay returns unique transition ids per UTC day in [from,to).
// Used by the lifecycle reconciler: PG rows are deleted only when the archive
// holds at least as many unique ids for that day (P10-004).
func (c *Client) CountTransitionsByDay(ctx context.Context, from, to time.Time) (map[string]int64, error) {
	q := `SELECT toString(toDate(transition_at, 'UTC')) AS day, uniqExact(transition_id) AS n
	      FROM alert_transitions FINAL
	      WHERE transition_at >= {from:DateTime64(3, 'UTC')}
	        AND transition_at < {to:DateTime64(3, 'UTC')}
	      GROUP BY day
	      FORMAT JSON`
	params := map[string]string{
		"param_from": from.UTC().Format(chTimeLayout),
		"param_to":   to.UTC().Format(chTimeLayout),
	}
	body, err := c.postBody(ctx, q, params)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Data []struct {
			Day string  `json:"day"`
			N   chCount `json:"n"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("clickhouse: decode day counts: %w", err)
	}
	out := make(map[string]int64, len(parsed.Data))
	for _, r := range parsed.Data {
		out[r.Day] = int64(r.N)
	}
	return out, nil
}
