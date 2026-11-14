package platform

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigEmptyPathFailsFast(t *testing.T) {
	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error for empty config path")
	}
}

func TestLoadConfigDefaultsAndFileValues(t *testing.T) {
	t.Setenv("LOG_LEVEL", "") // isolate env projection
	path := writeFile(t, `
api_addr: ":9090"
otlp_endpoint: ""
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.APIAddr != ":9090" {
		t.Errorf("api_addr from file = %q, want :9090", cfg.APIAddr)
	}
	// Fields absent from the file must fall back to default tags.
	if cfg.WorkerAddr != ":8081" || cfg.EdgeAddr != ":8082" {
		t.Errorf("defaults not applied: worker=%q edge=%q", cfg.WorkerAddr, cfg.EdgeAddr)
	}
	if cfg.APIOrigin != "http://127.0.0.1:8080" {
		t.Errorf("api_origin default = %q", cfg.APIOrigin)
	}
}

func TestLoadConfigEnvOverridesFile(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	path := writeFile(t, "api_addr: \":9090\"\n")
	t.Setenv("ALERT_API_ADDR", ":7070")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.APIAddr != ":7070" {
		t.Errorf("env override lost: api_addr = %q, want :7070", cfg.APIAddr)
	}
}

func TestLoadConfigLogLevelProjectsToEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	path := writeFile(t, "log_level: debug\n")
	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := os.Getenv("LOG_LEVEL"); got != "debug" {
		t.Errorf("LOG_LEVEL = %q, want debug (projected from YAML)", got)
	}
	// An explicitly set LOG_LEVEL must win over the YAML value.
	t.Setenv("LOG_LEVEL", "warn")
	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := os.Getenv("LOG_LEVEL"); got != "warn" {
		t.Errorf("LOG_LEVEL = %q, want warn (explicit env beats YAML)", got)
	}
}

func TestAddrFor(t *testing.T) {
	cfg := &Config{APIAddr: ":1", WorkerAddr: ":2", EdgeAddr: ":3"}
	for comp, want := range map[string]string{
		ComponentAPI: ":1", ComponentWorker: ":2", ComponentEdge: ":3",
	} {
		got, err := cfg.AddrFor(comp)
		if err != nil || got != want {
			t.Errorf("AddrFor(%s) = %q, %v; want %q", comp, got, err, want)
		}
	}
	if _, err := cfg.AddrFor("nope"); err == nil {
		t.Error("AddrFor(unknown) must fail")
	}
}
