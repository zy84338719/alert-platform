// Command alert-edge runs the edge component (docs/02 §2):
//
//	gateway mode   — vendor webhooks -> local auth -> translate -> WAL
//	                 (fsync before 202, EDGE-001) -> uploader -> central API.
//	collector mode — HUATUO SSE stream -> occurrence/gap handling -> WAL
//	                 (HT-001/003/004) -> uploader.
//
// The WAL is the durability boundary: the uploader retransmits with stable
// transport ids and the center's receipts absorb duplicates (EDGE-002).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/example/alert-platform/internal/adapter/inbound/http/edgegw"
	"github.com/example/alert-platform/internal/application/collector"
	"github.com/example/alert-platform/internal/application/edge"
	"github.com/example/alert-platform/internal/infrastructure/platform"
	"github.com/example/alert-platform/internal/infrastructure/sseclient"
	"github.com/example/alert-platform/internal/infrastructure/wal"
	oekitmetrics "github.com/oekit/metrics"
)

// version is injected at image build time via -ldflags.
var version = "0.1.0-dev"

func main() {
	cfgPath := flag.String("config", "etc/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := platform.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// ONE WAL instance shared by the receiver, collector and uploader.
	log, err := wal.Open(cfg.EdgeWALDir, int64(cfg.EdgeWALMaxBytes)<<20, 64<<20)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wal:", err)
		os.Exit(1)
	}

	run := func(ctx context.Context) error {
		// Disk waterline metrics (EDGE-003).
		walBytes := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "alert_platform", Subsystem: "edge", Name: "wal_bytes",
			Help: "Edge WAL size in bytes (EDGE-003 disk waterline).",
		})
		walPending := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "alert_platform", Subsystem: "edge", Name: "wal_pending_records",
			Help: "Edge WAL records awaiting center acknowledgement.",
		})
		oekitmetrics.Registry.MustRegister(walBytes, walPending)
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					b, p := log.Stats()
					walBytes.Set(float64(b))
					walPending.Set(float64(p))
				}
			}
		}()

		uploader, err := edge.NewUploader(log, edge.UploaderConfig{
			Endpoint:          cfg.APIOrigin + "/ingest/v1/events:batch",
			EdgeKey:           cfg.EdgeAPIKey, // P0-03: uploads send this as the bearer credential; multi-source edges add per-source SourceTokens
			RatePerSec:        cfg.EdgeUploadRate,
			ClientCertFile:    cfg.EdgeClientCert,
			ClientKeyFile:     cfg.EdgeClientKey,
			CAFile:            cfg.EdgeCAFile,
			HeartbeatURL:      cfg.APIOrigin + "/edge/v1/heartbeat",
			HeartbeatTenantID: cfg.HuatuoTenantID,
			HeartbeatSourceID: cfg.HeartbeatSourceID,
		})
		if err != nil {
			return err
		}
		go func() {
			if err := uploader.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "uploader:", err)
			}
		}()

		if cfg.EdgeMode == "collector" {
			stream := sseclient.New(cfg.HuatuoSSEURL)
			col := collector.New(adaptStream(stream), log, collector.Config{
				TenantID: cfg.HuatuoTenantID, SourceID: cfg.HuatuoSourceID, ClusterID: cfg.HuatuoClusterID,
			})
			return col.Run(ctx)
		}
		return nil // gateway handled by setup below
	}

	setup := func(h *server.Hertz) {
		if cfg.EdgeMode != "gateway" {
			return
		}
		gw, err := edgegw.NewGateway(cfg.EdgeSourcesFile, log)
		if err != nil {
			// A misconfigured gateway fails fast rather than accepting
			// pushes it cannot authenticate.
			panic("edge gateway: " + err.Error())
		}
		gw.Register(h)
	}

	if err := platform.RunServiceWith(cfg, platform.ComponentEdge, version, setup, run); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// adaptStream bridges the infrastructure client onto the collector port.
func adaptStream(s *sseclient.Client) collector.Stream {
	return &streamAdapter{c: s}
}

type streamAdapter struct{ c *sseclient.Client }

func (a *streamAdapter) Run(ctx context.Context, handle func(collector.Event) error) error {
	return a.c.Run(ctx, func(e sseclient.Event) error {
		return handle(collector.Event{
			ID: e.ID, Name: e.Name, Data: e.Data,
			Gap: e.Gap, GapFrom: e.GapFrom, GapTo: e.GapTo,
		})
	})
}

func (a *streamAdapter) LastID() string { return a.c.LastID() }
