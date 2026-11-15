// Command loadgen is the PERF-001 load generator: canonical batches via the
// real ingest HTTP path at a target EPS, recording per-batch push latency;
// afterwards it samples event_receipts.first_received_at against push
// completion to report end-to-end (push -> durably processed) P50/P95/P99.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	api := flag.String("api", "http://127.0.0.1:18300", "central api origin")
	eps := flag.Int("eps", 100, "events per second (steady)")
	duration := flag.Duration("d", 60*time.Second, "steady duration")
	burst := flag.Bool("burst", false, "burst mode: -eps becomes the burst rate")
	dsn := flag.String("pg", "", "PG DSN to sample end-to-end latency (optional)")
	flag.Parse()

	tenants := []string{
		"11112222-0000-0000-0000-000000000001",
		"11112222-0000-0000-0000-000000000002",
		"11112222-0000-0000-0000-000000000003",
	}
	sources := []string{
		"11112222-0000-0000-0000-0000000000a1",
		"11112222-0000-0000-0000-0000000000a2",
	}
	client := &http.Client{Timeout: 10 * time.Second}

	var mu sync.Mutex
	pushLat := []time.Duration{}
	pushedAt := map[string]time.Time{} // transport id -> push completion time
	var pushErrs int64

	start := time.Now()
	tickInterval := 500 * time.Millisecond
	perTick := float64(*eps) * tickInterval.Seconds()
	var wg sync.WaitGroup
	ticks := int(*duration / tickInterval)
	for t := 0; t < ticks; t++ {
		n := int(perTick + rand.Float64())
		// spread identity: 40% repeats of hot alerts (realistic state updates)
		events := make([]map[string]any, 0, n)
		transportIDs := make([]string, 0, n)
		now := time.Now().UTC()
		for i := 0; i < n; i++ {
			tenant := tenants[rand.Intn(len(tenants))]
			source := sources[rand.Intn(len(sources))]
			var uid string
			if rand.Float64() < 0.4 {
				uid = fmt.Sprintf("pod-hot-%d", rand.Intn(20)) // hot set: repeats
			} else {
				uid = fmt.Sprintf("pod-cold-%d-%d", t, i)
			}
			ev := map[string]any{
				"event_id":     fmt.Sprintf("lg-%d-%d", t, i),
				"transport_id": fmt.Sprintf("lg-%d-%d", t, i),
				"tenant_id":    tenant,
				"source":       map[string]any{"source_id": source, "source_type": 1, "external_rule_id": fmt.Sprintf("rule-%d", rand.Intn(50))},
				"resource":     map[string]any{"cluster_id": "perf", "resource_type": "pod", "resource_uid": uid},
				"kind":         1, "signal": 1, "severity": 1 + rand.Intn(3),
				"alert_name":  "LoadGen Alert",
				"occurred_at": map[string]int64{"seconds": now.Unix()},
				"received_at": map[string]int64{"seconds": now.Unix()},
				"starts_at":   map[string]int64{"seconds": now.Unix()},
				"event_key":   fmt.Sprintf("lg-%d-%d", t, i),
				"labels":      map[string]string{"ns": fmt.Sprintf("ns-%d", rand.Intn(8))},
			}
			events = append(events, ev)
			transportIDs = append(transportIDs, fmt.Sprintf("lg-%d-%d", t, i))
		}
		// chunk into batches of 50
		for len(events) > 0 {
			cut := 50
			if len(events) < cut {
				cut = len(events)
			}
			chunk := events[:cut]
			events = events[cut:]
			wg.Add(1)
			go func(chunk []map[string]any, ids []string) {
				defer wg.Done()
				body, _ := json.Marshal(map[string]any{
					"batch_id": fmt.Sprintf("lb-%d-%d", t, rand.Int63()),
					"events":   chunk,
				})
				t0 := time.Now()
				resp, err := client.Post(*api+"/ingest/v1/events:batch", "application/json", bytes.NewReader(body))
				if err != nil || resp.StatusCode != 202 {
					mu.Lock()
					pushErrs++
					mu.Unlock()
					if resp != nil {
						_ = resp.Body.Close()
					}
					return
				}
				_ = resp.Body.Close()
				d := time.Since(t0)
				mu.Lock()
				pushLat = append(pushLat, d)
				now := time.Now()
				for _, id := range ids {
					pushedAt[id] = now
				}
				mu.Unlock()
			}(chunk, transportIDs[:cut])
			transportIDs = transportIDs[cut:]
		}
		time.Sleep(tickInterval)
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("load: %d eps target, %s%s, %.1fs actual\n", *eps, *duration, map[bool]string{true: " (burst)", false: ""}[*burst], elapsed.Seconds())
	fmt.Printf("push: %d batches, errors=%d, P50=%s P95=%s P99=%s\n",
		len(pushLat), pushErrs, pct(pushLat, 50), pct(pushLat, 95), pct(pushLat, 99))

	if *dsn != "" {
		// sample end-to-end: first_received_at - push completion
		time.Sleep(3 * time.Second) // let stragglers land
		sample := sampleE2E(*dsn, pushedAt)
		if len(sample) > 0 {
			fmt.Printf("e2e (push->receipt): n=%d P50=%s P95=%s P99=%s\n",
				len(sample), pct(sample, 50), pct(sample, 95), pct(sample, 99))
		}
	}
	_ = strings.TrimSpace
	_ = os.Stdout
}

func pct(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration{}, ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (p*len(sorted) + 99) / 100
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

// sampleE2E queries receipts for the pushed transport ids and derives
// processing latency (first_received_at - push completion).
func sampleE2E(dsn string, pushedAt map[string]time.Time) []time.Duration {
	pool, err := openPG(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pg sample:", err)
		return nil
	}
	defer pool.Close()
	// hash-partition the ids to bound the IN list
	ids := make([]string, 0, len(pushedAt))
	for id := range pushedAt {
		ids = append(ids, id)
	}
	out := []time.Duration{}
	sampleSize := 2000
	if len(ids) > sampleSize {
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		ids = ids[:sampleSize]
	}
	for i := 0; i < len(ids); i += 500 {
		end := i + 500
		if end > len(ids) {
			end = len(ids)
		}
		rows, err := pool.Query(contextBG(),
			`SELECT transport_id, first_received_at FROM event_receipts WHERE transport_id = ANY($1)`, toTextArray(ids[i:end]))
		if err != nil {
			fmt.Fprintln(os.Stderr, "pg query:", err)
			return out
		}
		for rows.Next() {
			var id string
			var at time.Time
			_ = rows.Scan(&id, &at)
			if t, ok := pushedAt[id]; ok {
				if d := at.Sub(t); d >= 0 {
					out = append(out, d)
				}
			}
		}
		rows.Close()
	}
	return out
}
