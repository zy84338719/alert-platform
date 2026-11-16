package promclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// P9-MC-01：Prometheus query_range 参数与 matrix 解析。
func TestQueryRangeParsesMatrix(t *testing.T) {
	var query, start, end, step string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query_range" {
			t.Errorf("path = %s", r.URL.Path)
		}
		query = r.URL.Query().Get("query")
		start = r.URL.Query().Get("start")
		end = r.URL.Query().Get("end")
		step = r.URL.Query().Get("step")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"reason":"invalid"},"values":[[1700000000,"1.5"],[1700000060,"2"]]},
			{"metric":{"reason":"forbidden"},"values":[[1700000000,"0.5"]]}]}}`))
	}))
	defer srv.Close()

	c := New(srv.URL)
	at := time.Unix(1700000000, 0)
	series, err := c.QueryRange(context.Background(), `sum(rate(x[5m]))`, at.Add(-time.Hour), at, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 2 {
		t.Fatalf("series = %d, want 2", len(series))
	}
	if series[0].Labels["reason"] != "invalid" || len(series[0].Points) != 2 || series[0].Points[1][1] != 2 {
		t.Fatalf("series[0] = %+v", series[0])
	}
	if query != `sum(rate(x[5m]))` || start != "1699996400" || end != "1700000000" || step != "60s" {
		t.Fatalf("query params: query=%q start=%s end=%s step=%s", query, start, end, step)
	}
}

func TestQueryRangeErrors(t *testing.T) {
	at := time.Unix(1700000000, 0)
	window := []time.Duration{time.Minute}
	_ = window

	t.Run("http error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream down"))
		}))
		defer srv.Close()
		if _, err := New(srv.URL).QueryRange(context.Background(), "q", at.Add(-time.Hour), at, time.Minute); err == nil {
			t.Fatal("http error must surface")
		}
	})
	t.Run("prometheus error status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
		}))
		defer srv.Close()
		if _, err := New(srv.URL).QueryRange(context.Background(), "q", at.Add(-time.Hour), at, time.Minute); err == nil {
			t.Fatal("prometheus error status must surface")
		}
	})
	t.Run("not configured", func(t *testing.T) {
		if _, err := New("").QueryRange(context.Background(), "q", at.Add(-time.Hour), at, time.Minute); err == nil {
			t.Fatal("empty base URL must fail fast")
		}
	})
}
