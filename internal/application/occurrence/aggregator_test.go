package occurrence

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

// HT-003: threshold hits promote exactly once per burst window; cooldown
// prevents alert storms; the window slides.
func TestBurstPromotion(t *testing.T) {
	a := New(3, 5*time.Minute)
	occ := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }

	if a.Observe("oom/node1", occ(0)) {
		t.Fatal("first occurrence promoted early")
	}
	if a.Observe("oom/node1", occ(1)) {
		t.Fatal("second occurrence promoted early")
	}
	if !a.Observe("oom/node1", occ(2)) {
		t.Fatal("third occurrence within window must promote")
	}
	// Fourth hit inside the same burst: no second promotion (cooldown).
	if a.Observe("oom/node1", occ(3)) {
		t.Fatal("double promotion inside one burst")
	}
	// After the window cools, a NEW burst promotes again.
	if a.Observe("oom/node1", occ(20)) {
		t.Fatal("stale burst promoted")
	}
	a2 := New(3, 5*time.Minute)
	_ = a2
	// New burst after cooldown: 20,21,22 rebuild the count and 22 promotes
	// (cooldown from the first burst has long elapsed).
	if a.Observe("oom/node1", occ(21)) {
		t.Fatal("second occurrence of new burst promoted early")
	}
	if !a.Observe("oom/node1", occ(22)) {
		t.Fatal("new burst must promote after cooldown")
	}
	// Different keys are independent.
	if a.Observe("oom/node2", occ(22)) || a.Observe("oom/node2", occ(23)) {
		t.Fatal("node2 promoted early")
	}
}

// Count reflects the sliding window only.
func TestWindowSlides(t *testing.T) {
	a := New(100, 1*time.Minute)
	a.Observe("k", t0)
	a.Observe("k", t0.Add(10*time.Second))
	if got := a.Count("k", t0.Add(11*time.Second)); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	if got := a.Count("k", t0.Add(2*time.Minute)); got != 0 {
		t.Fatalf("count after window = %d, want 0", got)
	}
}

// P8-P1-06 失败测试：聚合器内存有界 ——
// ① 长期不再出现的 Key 由 Prune 主动淘汰（修复前 map 只增不减）；
// ② Key 数量配额：唯一键洪峰下按最久未观察顺序驱逐，Len 不超过上限。
func TestAggregatorPrunesExpiredKeys(t *testing.T) {
	a := New(3, 5*time.Minute)
	a.Observe("oom/node-old-1", t0)
	a.Observe("oom/node-old-2", t0)
	a.Observe("oom/node-live", t0.Add(4*time.Minute))
	if got := a.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	// 窗口（5m）过后：两个旧键淘汰，live 仍在窗口内。
	evicted := a.Prune(t0.Add(6 * time.Minute))
	if evicted != 2 {
		t.Fatalf("evicted = %d, want 2 (long-dead keys must not stay in memory)", evicted)
	}
	if a.Len() != 1 {
		t.Fatalf("Len after prune = %d, want 1", a.Len())
	}
	if a.Count("oom/node-live", t0.Add(6*time.Minute)) != 1 {
		t.Fatal("live key was evicted by the sweep")
	}
	// 全部过期后归零。
	if a.Prune(t0.Add(time.Hour)) != 1 || a.Len() != 0 {
		t.Fatalf("aggregator did not drain: len=%d", a.Len())
	}
}

func TestAggregatorKeyCapEvictsOldest(t *testing.T) {
	a := New(3, 5*time.Minute)
	a.SetMaxKeys(2)
	a.Observe("k1", t0)
	a.Observe("k2", t0.Add(time.Second))
	a.Observe("k3", t0.Add(2*time.Second)) // 超过配额 -> 驱逐最久未观察的 k1
	if a.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (key cap)", a.Len())
	}
	if a.Count("k1", t0.Add(2*time.Second)) != 0 {
		t.Fatal("oldest key was not evicted under cap pressure")
	}
	if a.Count("k2", t0.Add(2*time.Second)) != 1 || a.Count("k3", t0.Add(2*time.Second)) != 1 {
		t.Fatal("recent keys must survive the cap")
	}
}
