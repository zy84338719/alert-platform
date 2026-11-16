package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// P9-OBS-07：兜底自告警走独立 webhook（不经 notification_outbox），
// payload 带 source/component/severity/message；未配置时安全 no-op。
func TestSelfAlerterDelivers(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	alerter := NewSelfAlerter(srv.URL, true) // loopback allowed for the test
	alerter.Notify(context.Background(), "alert-worker", "critical background task exited: relay")

	if got["source"] != "alert-platform" || got["component"] != "alert-worker" ||
		got["severity"] != "critical" || got["message"] == "" || got["message"] == nil {
		t.Fatalf("unexpected self-alert payload: %+v", got)
	}
}

func TestSelfAlerterDisabled(t *testing.T) {
	if NewSelfAlerter("", false) != nil {
		t.Fatal("empty webhook must disable the alerter")
	}
	// nil receiver is safe (worker wiring always calls Notify).
	var nilAlerter *SelfAlerter
	nilAlerter.Notify(context.Background(), "alert-worker", "boom")
}

func TestSelfAlerterFailureIsBestEffort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	// Must not panic or block the caller: the process is already failing.
	NewSelfAlerter(srv.URL, true).Notify(context.Background(), "alert-worker", "boom")
}
