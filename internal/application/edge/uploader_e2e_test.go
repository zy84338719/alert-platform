package edge_test

// P0-02 (rh-p0-02) 失败测试③：断网补传端到端 —— 中央 API 拒掉批 b0（HTTP 400
// 非 429）但接受 b1/b2。uploader 逐条确认后，被拒的 b0 必须留在 WAL 里继续
// 重试，且 Compact 不得把它清掉（修复前：b2 的确认把水位抬过 b0，b0 被清掉，
// 断网缓冲告警丢失）；中央恢复后 b0 送达、全部批次零丢失。
// （外部测试包：infrastructure/wal 反向依赖 application/edge，本文件放
// edge_test 以免 import 环。）

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/example/alert-platform/internal/application/edge"
	"github.com/example/alert-platform/internal/infrastructure/wal"
)

func TestOutageReplayNoLossAroundRejectedRecord(t *testing.T) {
	var mu sync.Mutex
	rejected := map[string]bool{"b0": true} // 中央故障窗口内拒单
	var deliveries []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id, _ := body["batch_id"].(string)
		mu.Lock()
		isRejected := rejected[id]
		if !isRejected {
			deliveries = append(deliveries, id)
		}
		mu.Unlock()
		if isRejected {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":"batch %s rejected"}`, id)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	dir := t.TempDir()
	l, err := wal.Open(dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		body, _ := json.Marshal(map[string]any{"batch_id": fmt.Sprintf("b%d", i), "events": []any{}})
		if _, err := l.Append(wal.Record{ID: fmt.Sprintf("b%d", i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}

	u, err := edge.NewUploader(l, edge.UploaderConfig{Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	// 第一轮：b0 被拒、b1/b2 成功（跳跃确认场景）。
	sent, err := u.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if sent != 2 {
		t.Fatalf("sent = %d, want 2 (b1/b2 accepted)", sent)
	}

	// Compact 后 b0 必须还在（修复前：b2 的确认把水位抬到 2，b0 被清掉）。
	if err := l.Compact(); err != nil {
		t.Fatal(err)
	}
	pending, err := l.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != "b0" {
		t.Fatalf("pending after compact = %+v, want [b0] — the rejected batch was dropped", pending)
	}

	// 中央恢复：b0 重试成功，全部批次零丢失（每批恰好送达一次）。
	mu.Lock()
	rejected = map[string]bool{}
	mu.Unlock()
	for {
		sent, err := u.DrainOnce(context.Background())
		if err != nil {
			t.Fatalf("retry drain: %v", err)
		}
		pending, _ = l.Pending()
		if len(pending) == 0 {
			break
		}
		if sent == 0 {
			t.Fatalf("stuck: %d pending but nothing sent", len(pending))
		}
	}
	mu.Lock()
	defer mu.Unlock()
	sorted := append([]string(nil), deliveries...)
	sort.Strings(sorted)
	want := []string{"b0", "b1", "b2"}
	if fmt.Sprint(sorted) != fmt.Sprint(want) {
		t.Fatalf("deliveries = %v, want exactly %v — zero loss, zero unneeded retransmit", deliveries, want)
	}
}
