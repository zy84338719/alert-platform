// Package clickhouse is the HTTP-only ClickHouse adapter for the transition
// archive (P9-CH-01). It deliberately avoids a driver dependency: insert uses
// JSONEachRow, read uses FORMAT JSON, and tenant/time values travel as bound
// query parameters ({tenant:UUID} etc.) — never string-interpolated.
package clickhouse

import (
	"context"
	"encoding/json"
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
	q := `SELECT transition_id, instance_id, from_state, to_state, event_id, transition_at
	      FROM alert_transitions
	      WHERE tenant_id = {tenant:UUID}
	        AND transition_at >= {from:DateTime64(3)}
	        AND transition_at < {to:DateTime64(3)}
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
		return nil, fmt.Errorf("clickhouse: HTTP %d: %s", resp.StatusCode, snippet(respBody))
	}
	return respBody, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}
