// Package promclient implements the Prometheus HTTP API port used by the
// console metrics center (P9-MC-01). Only query_range is needed; the catalog
// service supplies the expression, never the caller.
package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/example/alert-platform/internal/application/metricsquery"
)

// Client talks to the Prometheus HTTP API.
type Client struct {
	base string
	hc   *http.Client
}

// New builds a client; empty base yields a client that fails all queries.
func New(base string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), hc: &http.Client{Timeout: 10 * time.Second}}
}

// maxSeries bounds the response size (defense against runaway cardinality).
const maxSeries = 20

type matrixResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string    `json:"metric"`
			Values [][2]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// QueryRange calls /api/v1/query_range and normalizes the matrix.
func (c *Client) QueryRange(ctx context.Context, expr string, start, end time.Time, step time.Duration) ([]metricsquery.Series, error) {
	if c.base == "" {
		return nil, fmt.Errorf("prometheus: base URL not configured")
	}
	q := url.Values{}
	q.Set("query", expr)
	q.Set("start", strconv.FormatInt(start.Unix(), 10))
	q.Set("end", strconv.FormatInt(end.Unix(), 10))
	q.Set("step", fmt.Sprintf("%ds", int(step.Seconds())))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/v1/query_range?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("prometheus: build request: %w", err)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus: query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("prometheus: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus: HTTP %d: %s", resp.StatusCode, snippet(body))
	}
	var parsed matrixResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("prometheus: decode response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s error: %s", parsed.ErrorType, parsed.Error)
	}
	if parsed.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("prometheus: unexpected resultType %q", parsed.Data.ResultType)
	}
	out := make([]metricsquery.Series, 0, len(parsed.Data.Result))
	for _, item := range parsed.Data.Result {
		if len(out) >= maxSeries {
			break
		}
		points, err := parseValues(item.Values)
		if err != nil {
			return nil, err
		}
		out = append(out, metricsquery.Series{Labels: item.Metric, Points: points})
	}
	return out, nil
}

func parseValues(values [][2]json.RawMessage) ([][2]float64, error) {
	points := make([][2]float64, 0, len(values))
	for _, v := range values {
		var ts float64
		if err := json.Unmarshal(v[0], &ts); err != nil {
			return nil, fmt.Errorf("prometheus: sample timestamp: %w", err)
		}
		var raw string
		if err := json.Unmarshal(v[1], &raw); err != nil {
			return nil, fmt.Errorf("prometheus: sample value: %w", err)
		}
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("prometheus: parse sample %q: %w", raw, err)
		}
		points = append(points, [2]float64{ts, val})
	}
	return points, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
