package clickhouse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/alert-platform/internal/application/archive"
)

// P9-CH-01：INSERT 走 JSONEachRow + database 参数；查询用绑定参数（不拼 SQL）。
func TestInsertTransitions(t *testing.T) {
	var query, database, auth, body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		database = r.URL.Query().Get("database")
		auth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "alert_archive", "ch-user", "ch-pass")
	at := time.Date(2026, 11, 16, 10, 0, 0, 123000000, time.UTC)
	err := c.InsertTransitions(context.Background(), []archive.Transition{{
		TenantID:     "11111111-1111-1111-1111-111111111111",
		TransitionID: "22222222-2222-2222-2222-222222222222",
		InstanceID:   "33333333-3333-3333-3333-333333333333",
		FromState:    "firing", ToState: "resolved", EventID: "e-1", At: at,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "INSERT INTO alert_transitions FORMAT JSONEachRow") {
		t.Fatalf("query = %q", query)
	}
	if database != "alert_archive" || !strings.HasPrefix(auth, "Basic ") {
		t.Fatalf("database=%q auth=%q", database, auth)
	}
	for _, want := range []string{`"tenant_id":"11111111-1111-1111-1111-111111111111"`,
		`"transition_at":"2026-11-16 10:00:00.123"`, `"to_state":"resolved"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("insert body missing %s: %s", want, body)
		}
	}
}

func TestListTransitionsRange(t *testing.T) {
	var query, tenant, from, to, lim string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		query, tenant, from, to, lim = q.Get("query"), q.Get("param_tenant"), q.Get("param_from"), q.Get("param_to"), q.Get("param_lim")
		_, _ = w.Write([]byte(`{"data":[
			{"transition_id":"t-1","instance_id":"i-1","from_state":"firing","to_state":"resolved","event_id":"e-1","transition_at":"2026-11-16 10:00:00.123"},
			{"transition_id":"t-2","instance_id":"i-2","from_state":"","to_state":"firing","event_id":"e-2","transition_at":"2026-11-16 09:00:00.000"}]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "", "")
	rows, err := c.ListTransitionsRange(context.Background(), "11111111-1111-1111-1111-111111111111",
		time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC), time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ToState != "resolved" || rows[1].FromState != "" {
		t.Fatalf("rows = %+v", rows)
	}
	if !strings.Contains(query, "{tenant:UUID}") || tenant != "11111111-1111-1111-1111-111111111111" ||
		from != "2026-11-16 08:00:00.000" || to != "2026-11-16 11:00:00.000" || lim != "100" {
		t.Fatalf("params query=%q tenant=%q from=%q to=%q lim=%q", query, tenant, from, to, lim)
	}
}

func TestErrorsSurface(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Code: 62. DB::Exception"))
	}))
	defer srv.Close()
	c := New(srv.URL, "", "", "")
	if err := c.InsertTransitions(context.Background(), []archive.Transition{{TenantID: "t"}}); err == nil {
		t.Fatal("HTTP 500 must surface")
	}
	if _, err := c.ListTransitionsRange(context.Background(), "t", time.Now().Add(-time.Hour), time.Now(), 10); err == nil {
		t.Fatal("HTTP 500 must surface on read")
	}
}

// AUD-001（#10）：INSERT 必须带 wait_end_of_query；执行期异常（200 正文
// 注入 Code: … DB::Exception）不得被判成功；4xx 归类 permanent，避免每
// tick 无限重试。
func TestInsertWaitsAndDetectsBodyException(t *testing.T) {
	var sawWait string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawWait = r.URL.Query().Get("wait_end_of_query")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Code: 241. DB::Exception: Memory limit exceeded"))
	}))
	defer srv.Close()

	c := New(srv.URL, "db", "", "")
	err := c.InsertTransitions(context.Background(), []archive.Transition{{TenantID: "t", TransitionID: "u"}})
	if sawWait != "1" {
		t.Fatalf("wait_end_of_query = %q, want 1", sawWait)
	}
	if err == nil {
		t.Fatal("200 body carrying DB::Exception must be an error (silent batch loss)")
	}
	if IsPermanent(err) {
		t.Fatalf("execution-time exception should be retryable, got permanent: %v", err)
	}
}

func TestClient4xxIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Code: 27. DB::Exception: Cannot parse input"))
	}))
	defer srv.Close()
	c := New(srv.URL, "", "", "")
	if err := c.InsertTransitions(context.Background(), []archive.Transition{{TenantID: "t"}}); !IsPermanent(err) {
		t.Fatalf("4xx error = %v, want PermanentError", err)
	}
}
