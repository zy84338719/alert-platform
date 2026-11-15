// Package platform is the shared process base for all three binaries
// (BASE-002): configuration loading, structured logging, health checks,
// metrics exposure and graceful bootstrap — wired once, used by
// alert-api / alert-worker / alert-edge.
//
// It lives in the infrastructure layer (docs/02 §3): domain and application
// code never import this package.
package platform

import (
	"fmt"
	"os"

	"github.com/oekit/core/config"
)

// Config is the flat per-process configuration shared by all binaries.
// Precedence (oekit core/config decode line): env tag > YAML file > default tag.
type Config struct {
	// LogLevel controls logx globally via LOG_LEVEL (DEBUG|INFO|WARN|ERROR).
	LogLevel string `yaml:"log_level" env:"LOG_LEVEL" default:"info"`

	// OTLPEndpoint is the OTLP gRPC collector address. Empty disables exporter
	// creation entirely (local/dev: trace_id still flows into logs, nothing
	// dials out).
	OTLPEndpoint string `yaml:"otlp_endpoint" env:"OTLP_ENDPOINT" default:""`

	// APIAddr is the alert-api listen address.
	APIAddr string `yaml:"api_addr" env:"ALERT_API_ADDR" default:":8080"`
	// WorkerAddr is the alert-worker health/metrics listen address.
	WorkerAddr string `yaml:"worker_addr" env:"ALERT_WORKER_ADDR" default:":8081"`
	// EdgeAddr is the alert-edge health/metrics listen address.
	EdgeAddr string `yaml:"edge_addr" env:"ALERT_EDGE_ADDR" default:":8082"`
	// APIOrigin is where alert-edge finds the central API (edge-side calls).
	APIOrigin string `yaml:"api_origin" env:"ALERT_API_ORIGIN" default:"http://127.0.0.1:8080"`

	// KafkaBrokers is the comma-separated bootstrap list for the event topic.
	KafkaBrokers string `yaml:"kafka_brokers" env:"ALERT_KAFKA_BROKERS" default:"127.0.0.1:9092"`
	// KafkaTopic is the canonical event topic (BASE-006).
	KafkaTopic string `yaml:"kafka_topic" env:"ALERT_KAFKA_TOPIC" default:"alert.events"`
	// KafkaLifecycleTopic receives relayed domain events (PROC-003, docs/04 §1).
	KafkaLifecycleTopic string `yaml:"kafka_lifecycle_topic" env:"ALERT_KAFKA_LIFECYCLE_TOPIC" default:"alert.lifecycle.v1"`
	// KafkaDLQTopic quarantines poison messages (PROC-001, docs/04 §1).
	KafkaDLQTopic string `yaml:"kafka_dlq_topic" env:"ALERT_KAFKA_DLQ_TOPIC" default:"alert.dlq.v1"`
	// KafkaGroupID is the worker consumer group.
	KafkaGroupID string `yaml:"kafka_group_id" env:"ALERT_KAFKA_GROUP" default:"alert-worker-dev"`

	// NotifyMode (REL-001): active|shadow|off — notification authority switch.
	NotifyMode string `yaml:"notify_mode" env:"ALERT_NOTIFY_MODE" default:"active"`

	// Notification delivery (NOTI-002): allowing private channel endpoints is
	// a dev/test-only escape hatch for the SSRF dial guard (T-013).
	AllowPrivateChannels bool `yaml:"allow_private_channels" env:"ALERT_ALLOW_PRIVATE_CHANNELS" default:"false"`

	// RetentionReceipts is the event-receipt retention window (RETN-001);
	// must exceed the idempotency horizon. CleanupInterval is its tick.
	RetentionReceipts string `yaml:"retention_receipts" env:"ALERT_RETENTION_RECEIPTS" default:"168h"`
	RetentionInterval string `yaml:"retention_interval" env:"ALERT_RETENTION_INTERVAL" default:"1h"`

	// StaleAfter is the source-silence threshold before active alerts go
	// stale (PROC-006); ReconcileInterval is the scheduler tick.
	StaleAfter        string `yaml:"stale_after" env:"ALERT_STALE_AFTER" default:"5m"`
	ReconcileInterval string `yaml:"reconcile_interval" env:"ALERT_RECONCILE_INTERVAL" default:"30s"`

	// Edge (EDGE-001~004 / HT): mode, WAL bounds, uploader and HUATUO stream.
	EdgeMode        string `yaml:"edge_mode" env:"ALERT_EDGE_MODE" default:"gateway"`
	EdgeWALDir      string `yaml:"edge_wal_dir" env:"ALERT_EDGE_WAL_DIR" default:".dev/edge-wal"`
	EdgeWALMaxBytes int    `yaml:"edge_wal_max_mb" env:"ALERT_EDGE_WAL_MAX_MB" default:"512"` // MiB
	EdgeSourcesFile string `yaml:"edge_sources_file" env:"ALERT_EDGE_SOURCES_FILE" default:"etc/edge-sources.json"`
	EdgeAPIKey      string `yaml:"edge_api_key" env:"ALERT_EDGE_API_KEY" default:""`
	// Edge uplink identity (P8-P0-02): ONE center credential scoped to a
	// tenant + allowed source subset. Preferred over per-source tokens.
	EdgeUplinkToken string `yaml:"edge_uplink_token" env:"ALERT_EDGE_UPLINK_TOKEN" default:""`
	EdgeID          string `yaml:"edge_id" env:"ALERT_EDGE_ID" default:""`
	// EdgeSourceTokens is a JSON object {source_id: token} of per-source upload
	// credentials for multi-source edges (legacy fallback when no uplink token).
	// JSON string keeps YAML and env (ALERT_EDGE_SOURCE_TOKENS) symmetric.
	EdgeSourceTokens string  `yaml:"edge_source_tokens" env:"ALERT_EDGE_SOURCE_TOKENS" default:""`
	EdgeUploadRate   float64 `yaml:"edge_upload_rate" env:"ALERT_EDGE_UPLOAD_RATE" default:"20"` // uploads/sec cap
	EdgeClientCert   string  `yaml:"edge_client_cert" env:"ALERT_EDGE_CLIENT_CERT" default:""`
	EdgeClientKey    string  `yaml:"edge_client_key" env:"ALERT_EDGE_CLIENT_KEY" default:""`
	EdgeCAFile       string  `yaml:"edge_ca_file" env:"ALERT_EDGE_CA_FILE" default:""`

	// HeartbeatSourceID identifies this edge to the center waterline (MC-002).
	HeartbeatSourceID string `yaml:"heartbeat_source_id" env:"ALERT_HEARTBEAT_SOURCE_ID" default:""`

	// HUATUO collector (HT-001).
	HuatuoSSEURL string `yaml:"huatuo_sse_url" env:"ALERT_HUATUO_SSE_URL" default:""`
	// P0-04: official /v1/events/watch is POST + JSON body + Bearer token.
	HuatuoToken     string `yaml:"huatuo_token" env:"ALERT_HUATUO_TOKEN" default:""`
	HuatuoTracer    string `yaml:"huatuo_tracer_filter" env:"ALERT_HUATUO_TRACER_FILTER" default:""`
	HuatuoTenantID  string `yaml:"huatuo_tenant_id" env:"ALERT_HUATUO_TENANT_ID" default:""`
	HuatuoSourceID  string `yaml:"huatuo_source_id" env:"ALERT_HUATUO_SOURCE_ID" default:""`
	HuatuoClusterID string `yaml:"huatuo_cluster_id" env:"ALERT_HUATUO_CLUSTER_ID" default:""`

	// PGDSN is the PostgreSQL connection string for the worker's store.
	PGDSN string `yaml:"pg_dsn" env:"ALERT_PG_DSN" default:"postgres://postgres:postgres@127.0.0.1:5432/alert_platform?sslmode=disable"`
}

// Component names used for service.name, logs and bootstrap resources.
const (
	ComponentAPI    = "alert-api"
	ComponentWorker = "alert-worker"
	ComponentEdge   = "alert-edge"
)

// AddrFor returns the listen address for the given component.
func (c *Config) AddrFor(component string) (string, error) {
	switch component {
	case ComponentAPI:
		return c.APIAddr, nil
	case ComponentWorker:
		return c.WorkerAddr, nil
	case ComponentEdge:
		return c.EdgeAddr, nil
	default:
		return "", fmt.Errorf("unknown component %q", component)
	}
}

// LoadConfig reads the YAML file at path (default/env tags still apply to
// fields absent from the file). Path must not be empty — the shipped dev
// default is etc/config.yaml; fail-fast on missing config is intentional.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("config path is empty (default: etc/config.yaml)")
	}
	cfg := &Config{}
	if err := config.LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("load config %s: %w", path, err)
	}
	// LoadFile does not run env injection itself (core#59): project env tags
	// over the file values so env > yaml > default holds end to end.
	if err := config.ApplyEnvOverlay(cfg); err != nil {
		return nil, fmt.Errorf("env overlay for %s: %w", path, err)
	}
	// logx reads LOG_LEVEL from the environment; a YAML-provided level must
	// therefore be projected into the env — unless LOG_LEVEL is already set
	// explicitly, which wins (env-tag precedence stays intact).
	if cfg.LogLevel != "" && os.Getenv("LOG_LEVEL") == "" {
		os.Setenv("LOG_LEVEL", cfg.LogLevel)
	}
	return cfg, nil
}
