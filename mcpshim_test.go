package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestShimManifestRetriesCachesAndFallsBack(t *testing.T) {
	var requests atomic.Int64
	manifest := Manifest{
		Protocol: ManifestProtocol,
		Name:     "courier-test",
		Version:  "1.2.3",
		Tools:    []ToolDef{{Name: "read_message"}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "not ready"})
			return
		}
		_ = json.NewEncoder(w).Encode(manifest)
	}))
	cacheDir := t.TempDir()
	var sleeps []time.Duration
	got, err := shimLoadManifest(context.Background(), shimManifestLoadOptions{
		HostURL:  server.URL,
		CacheDir: cacheDir,
		Client:   server.Client(),
		Sleep: func(_ context.Context, delay time.Duration) error {
			sleeps = append(sleeps, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, manifest) {
		t.Fatalf("manifest = %#v, want %#v", got, manifest)
	}
	if want := []time.Duration{shimManifestRetryWait, shimManifestRetryWait}; !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("retry sleeps = %v, want %v", sleeps, want)
	}
	if _, err := os.Stat(shimCachePath(cacheDir, server.URL)); err != nil {
		t.Fatalf("manifest cache: %v", err)
	}

	server.Close()
	sleeps = nil
	var logs []string
	cached, err := shimLoadManifest(context.Background(), shimManifestLoadOptions{
		HostURL:  server.URL,
		CacheDir: cacheDir,
		Client:   server.Client(),
		Sleep: func(_ context.Context, delay time.Duration) error {
			sleeps = append(sleeps, delay)
			return nil
		},
		Log: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cached, manifest) {
		t.Fatalf("cached manifest = %#v, want %#v", cached, manifest)
	}
	if len(sleeps) != shimManifestRetries {
		t.Fatalf("cache fallback attempts slept %d times, want %d", len(sleeps), shimManifestRetries)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "using cached manifest") {
		t.Fatalf("cache fallback logs = %q", logs)
	}
}

func TestShimServesManifestToolsAndOverridesAgent(t *testing.T) {
	var posted map[string]any
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/tool/read_message" {
			t.Fatalf("tool path = %q", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(shimToolResponse{Text: "delivery belongs to another agent", IsError: true})
	}))
	defer host.Close()

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"delivery_id": map[string]any{"type": "string"},
		},
		"required": []any{"delivery_id"},
	}
	manifest := Manifest{
		Protocol:     ManifestProtocol,
		Name:         "courier-test",
		Version:      "1.0.0",
		Instructions: "Always read the full message.",
		Tools: []ToolDef{{
			Name:        "read_message",
			Description: "Read one message",
			InputSchema: schema,
		}},
	}
	server := shimNewServer(manifest, MCPOptions{Agent: "trusted-agent", HostURL: host.URL}, host.Client())
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "shim-test", Version: "1.0.0"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	listed, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 1 {
		t.Fatalf("listed tools = %d, want 1", len(listed.Tools))
	}
	listedTool := listed.Tools[0]
	if listedTool.Name != manifest.Tools[0].Name || listedTool.Description != manifest.Tools[0].Description {
		t.Fatalf("listed tool = %#v", listedTool)
	}
	wantSchema, _ := json.Marshal(schema)
	gotSchema, _ := json.Marshal(listedTool.InputSchema)
	if string(gotSchema) != string(wantSchema) {
		t.Fatalf("input schema = %s, want %s", gotSchema, wantSchema)
	}

	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: "read_message",
		Arguments: map[string]any{
			"delivery_id": "delivery-1",
			"agent":       "attacker-controlled",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError || len(result.Content) != 1 {
		t.Fatalf("tool result = %#v", result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok || text.Text != "delivery belongs to another agent" {
		t.Fatalf("tool content = %#v", result.Content)
	}
	if posted["agent"] != "trusted-agent" || posted["delivery_id"] != "delivery-1" {
		t.Fatalf("posted args = %#v", posted)
	}
}

func TestShimProtocolMismatchWarnsWithoutRefusing(t *testing.T) {
	var logs []string
	shimWarnProtocol(Manifest{Protocol: ManifestProtocol + 1}, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if len(logs) != 1 || !strings.Contains(logs[0], "WARNING") || !strings.Contains(logs[0], "got 3") {
		t.Fatalf("mismatch logs = %q", logs)
	}

	logs = nil
	shimWarnProtocol(Manifest{Protocol: ManifestProtocol}, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if len(logs) != 0 {
		t.Fatalf("matching protocol logs = %q", logs)
	}
}

func TestShimRunServerExitsCleanlyOnStdinEOF(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "eof-test", Version: "1.0.0"}, nil)
	done := make(chan error, 1)
	go func() {
		done <- shimRunServer(context.Background(), server, strings.NewReader(""), io.Discard)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stdin EOF: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shim did not exit after stdin EOF")
	}
}
