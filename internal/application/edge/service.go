// Package edge implements the alert-edge use cases (EDGE-002): drain WAL
// records to the central API with stable transport ids, exponential backoff
// on failures, a rate cap, and TLS/mTLS configuration. The center's receipts
// layer absorbs re-sends of already-acked records (at-least-once, idempotent).
package edge

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/oekit/logx"
)

// WAL is the local buffer port (infrastructure/wal).
type WAL interface {
	Pending() ([]Record, error)
	Ack(seq uint64)
	Compact() error
	Stats() (bytes int64, pending int)
}

// Record mirrors wal.Record (kept local so the application layer owns the port).
type Record struct {
	ID       string `json:"id"`
	Body     []byte `json:"body"`
	Seq      uint64 `json:"seq"`
	SourceID string `json:"source_id"`
}

// Uploader drains one WAL into the central ingest endpoint.
type Uploader struct {
	wal          WAL
	client       *http.Client
	endpoint     string // central /ingest/v1/events:batch URL
	batch        int
	rateEvery    time.Duration     // min spacing between uploads (rate cap)
	edgeKey      string            // default upload credential + heartbeat key
	uplinkToken  string            // edge uplink identity (P8-P0-02), preferred
	edgeID       string            // exported in X-Edge-Id when uplink identity is used
	sourceTokens map[string]string // per-source upload credentials (P0-03)

	// Heartbeat (MC-002): report the WAL waterline to the center every minute.
	HeartbeatURL   string
	HeartbeatIdent struct {
		TenantID string
		SourceID string
	}
}

// UploaderConfig wires the uploader.
type UploaderConfig struct {
	Endpoint string
	// EdgeKey authenticates uploads and heartbeats (P0-03: uploads send it as
	// the Authorization bearer — the server binds the payload identity to the
	// credential; X-Edge-Key stays heartbeat-only).
	EdgeKey string
	// UplinkToken (P8-P0-02) is the edge-level upload credential: ONE token
	// authorized for a tenant and an allowed source subset on the center. It
	// takes precedence over SourceTokens/EdgeKey for uploads; EdgeID identifies
	// the credential in the X-Edge-Id header.
	UplinkToken string
	EdgeID      string
	// SourceTokens overrides the bearer per buffered source id
	// (multi-source edges: one credential per vendor source). Missing entries
	// fall back to EdgeKey.
	SourceTokens map[string]string
	Batch        int
	// RatePerSec caps uploads per second (EDGE-002 限速上限).
	RatePerSec float64
	// TLS: client cert/key + CA for mutual auth against the center.
	ClientCertFile, ClientKeyFile, CAFile string
	InsecureSkipVerify                    bool

	// Heartbeat (MC-002): center endpoint + identity for waterline reports.
	HeartbeatURL      string
	HeartbeatTenantID string
	HeartbeatSourceID string
}

// NewUploader builds the uploader (TLS files optional).
func NewUploader(w WAL, cfg UploaderConfig) (*Uploader, error) {
	// P8-P0-02: the uplink identity is a pair — the central API switches to the
	// edge-credential path on X-Edge-Id, so a token without an id (or an id
	// without a token) would authenticate against the wrong path and 401 at
	// runtime. Fail at startup instead of failing every upload.
	if (cfg.UplinkToken == "") != (cfg.EdgeID == "") {
		return nil, fmt.Errorf("edge uploader: UplinkToken and EdgeID must be configured together")
	}
	transport := &http.Transport{MaxIdleConns: 4}
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify} //nolint:gosec
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("edge client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("edge ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("edge ca: no certificates parsed")
		}
		tlsCfg.RootCAs = pool
	}
	transport.TLSClientConfig = tlsCfg
	every := time.Duration(0)
	if cfg.RatePerSec > 0 {
		every = time.Duration(float64(time.Second) / cfg.RatePerSec)
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 50
	}
	u := &Uploader{
		wal: w, endpoint: cfg.Endpoint, batch: batch, rateEvery: every, edgeKey: cfg.EdgeKey,
		uplinkToken: cfg.UplinkToken, edgeID: cfg.EdgeID,
		sourceTokens: cfg.SourceTokens,
		client:       &http.Client{Timeout: 15 * time.Second, Transport: transport},
	}
	if cfg.HeartbeatURL != "" {
		u.HeartbeatURL = cfg.HeartbeatURL
		u.HeartbeatIdent.TenantID = cfg.HeartbeatTenantID
		u.HeartbeatIdent.SourceID = cfg.HeartbeatSourceID
	}
	return u, nil
}

// Run drains until ctx is done. On failure the record stays pending and the
// next tick retries with exponential backoff (bounded to the tick interval —
// the WAL is the durable queue; losing pace is visible via Stats()).
func (u *Uploader) Run(ctx context.Context) error {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	hb := time.NewTicker(time.Minute)
	defer hb.Stop()
	if u.HeartbeatURL != "" {
		u.heartbeat(ctx) // first report lands immediately
	}
	var lastAttempt time.Time
	backoff := time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-hb.C:
			if u.HeartbeatURL != "" {
				u.heartbeat(ctx)
			}
		case <-tick.C:
			if u.rateEvery > 0 && time.Since(lastAttempt) < u.rateEvery {
				continue
			}
			sent, err := u.DrainOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					logx.CtxWarn(ctx, "upload failed; will retry (records stay in WAL)",
						logx.ErrKey, err, "backoff", backoff.String())
				}
				// simple bounded backoff between failed drains
				sleep := backoff
				if sleep < time.Second {
					backoff *= 2
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(sleep):
				}
				continue
			}
			backoff = time.Millisecond
			if sent > 0 {
				lastAttempt = time.Now()
			}
		}
	}
}

// DrainOnce pushes up to batch pending records as one PushBatch each (the WAL
// record body IS a serialized PushBatchRequest — transport id = record id).
func (u *Uploader) DrainOnce(ctx context.Context) (int, error) {
	pending, err := u.wal.Pending()
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	if len(pending) > u.batch {
		pending = pending[:u.batch]
	}
	sent := 0
	var maxSeq uint64
	for _, rec := range pending {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.endpoint, bytes.NewReader(rec.Body))
		if err != nil {
			return sent, err
		}
		req.Header.Set("Content-Type", "application/json")
		// P0-03/P8-P0-02: the center authenticates the upload credential and
		// binds the payload identity to it. Credential preference:
		//   1) edge uplink token (one identity scoped to allowed sources);
		//   2) per-source token (multi-source edge, legacy);
		//   3) shared edge key (single-source fallback).
		// The edge identity travels in X-Edge-Id so the center picks the edge
		// auth path deterministically.
		if u.edgeID != "" {
			req.Header.Set("X-Edge-Id", u.edgeID)
		}
		if tok := u.tokenFor(rec.SourceID); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return sent, fmt.Errorf("post batch %s: %w", rec.ID, err)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		_ = resp.Body.Close()
		n := len(body)
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			// 5xx/429: retry later; 4xx (except 429): the center rejected the
			// payload — retrying cannot fix it, but dropping silently is
			// worse; keep it pending and log loudly for an operator.
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
				return sent, fmt.Errorf("center returned %d for batch %s", resp.StatusCode, rec.ID)
			}
			logx.CtxError(ctx, "center rejected batch (kept pending for operator)",
				"batch", rec.ID, "status", resp.StatusCode, "body", string(body[:n]))
			continue
		}
		u.wal.Ack(rec.Seq)
		maxSeq = rec.Seq
		sent++
	}
	if maxSeq > 0 {
		if err := u.wal.Compact(); err != nil {
			return sent, err
		}
	}
	return sent, nil
}

// tokenFor resolves the upload credential for one buffered source.
// P8-P0-02: an edge uplink token (single identity scoped to an allowed source
// set) wins over per-source tokens and the shared edge key.
func (u *Uploader) tokenFor(sourceID string) string {
	if u.uplinkToken != "" {
		return u.uplinkToken
	}
	if sourceID != "" {
		if tok, ok := u.sourceTokens[sourceID]; ok && tok != "" {
			return tok
		}
	}
	return u.edgeKey
}

// Stats exposes the buffer waterline (EDGE-003 磁盘水位指标).
func (u *Uploader) Stats() (int64, int) { return u.wal.Stats() }

// heartbeat reports the WAL waterline to the center (MC-002: Buffer/Lag 上屏).
func (u *Uploader) heartbeat(ctx context.Context) {
	b, p := u.wal.Stats()
	body, _ := json.Marshal(map[string]any{
		"tenant_id": u.HeartbeatIdent.TenantID, "source_id": u.HeartbeatIdent.SourceID,
		"wal_bytes": b, "wal_pending": p,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.HeartbeatURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if u.edgeKey != "" {
		req.Header.Set("X-Edge-Key", u.edgeKey)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		logx.CtxWarn(ctx, "edge heartbeat failed", logx.ErrKey, err)
		return
	}
	_ = resp.Body.Close()
}

// Once wiring helper for tests.
var _ = sync.Once{}
