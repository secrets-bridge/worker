package main

import (
	"bytes"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// TestRetiredEnv_GitOpsIsNotASwitch: the settings of the removed Argo
// CD poller are reported by name when set, their values never reach the
// log, and no value, valid or not, changes the configuration or stops
// the boot (secrets-bridge/gateway#13).
func TestRetiredEnv_GitOpsIsNotASwitch(t *testing.T) {
	base, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig with nothing set: %v", err)
	}
	if len(base.RetiredEnvSet) != 0 {
		t.Fatalf("RetiredEnvSet = %v with nothing set", base.RetiredEnvSet)
	}
	var quiet bytes.Buffer
	logRetiredEnv(slog.New(slog.NewTextHandler(&quiet, nil)), base.RetiredEnvSet)
	if quiet.Len() != 0 {
		t.Fatalf("logged %q with nothing set", quiet.String())
	}

	values := map[string]string{
		"SB_WORKER_GITOPS_ENABLED":          "true",
		"SB_WORKER_GITOPS_POLL_INTERVAL":    "-1s",
		"SB_WORKER_GITOPS_TIMEOUT_INTERVAL": "retired-timeout-canary",
		"SB_WORKER_GITOPS_BATCH_SIZE":       "0",
		"SB_WORKER_GITOPS_HTTP_TIMEOUT":     "retired-http-canary",
	}
	for k, v := range values {
		t.Setenv(k, v)
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("a retired setting stopped the boot: %v", err)
	}
	if !slices.Equal(cfg.RetiredEnvSet, retiredEnv) {
		t.Fatalf("RetiredEnvSet = %v, want %v", cfg.RetiredEnvSet, retiredEnv)
	}
	got := cfg
	got.RetiredEnvSet = nil
	if !configEqual(got, base) {
		t.Fatalf("a retired setting changed the configuration:\n got %+v\nwant %+v", got, base)
	}
	var buf bytes.Buffer
	logRetiredEnv(slog.New(slog.NewTextHandler(&buf, nil)), cfg.RetiredEnvSet)
	out := buf.String()
	for name, v := range values {
		if !strings.Contains(out, name) {
			t.Errorf("log %q does not name %s", out, name)
		}
		if strings.Contains(v, "canary") && strings.Contains(out, v) {
			t.Errorf("log leaks a retired value: %q", out)
		}
	}
}

func configEqual(a, b Config) bool {
	a.RetiredEnvSet, b.RetiredEnvSet = nil, nil
	return reflect.DeepEqual(a, b)
}
