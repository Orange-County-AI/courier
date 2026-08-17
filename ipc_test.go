package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func newHosttoolsIPCHandler(t *testing.T, h *hosttoolsHarness) http.Handler {
	t.Helper()
	handler, err := NewIPCHandler(IPCOptions{
		Store: h.store,
		Manifest: func() (Manifest, error) {
			return BuildManifest(BuildManifestOptions{Name: "courier-test", Connectors: h.registry.All()})
		},
		CallTool: func(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
			for _, hostName := range HostToolNames {
				if name == hostName {
					return h.tools.CallTool(ctx, name, args)
				}
			}
			for _, connector := range h.registry.All() {
				for _, tool := range connector.ManifestTools() {
					if tool.Name == name {
						return connector.CallTool(ctx, name, args)
					}
				}
			}
			return ToolResult{}, errors.New("unknown tool: " + name)
		},
		Health: func() HealthState {
			return HealthState{
				Org:        "test-org",
				Target:     h.target,
				Shadow:     false,
				Connectors: []string{h.connector.name},
				HostTools:  append([]string(nil), HostToolNames...),
			}
		},
		Now: func() int64 { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func ipcRequest(t *testing.T, client *http.Client, method, endpoint string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if strings.Contains(response.Header.Get("Content-Type"), "application/json") {
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
	}
	response.Body.Close()
	return response, decoded
}

func startIPCServer(t *testing.T, listener net.Listener, handler http.Handler) {
	t.Helper()
	server := &http.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown IPC server: %v", err)
		}
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve IPC: %v", err)
		}
	})
}

func TestIPCServesHealthAndManifest(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	handler := newHosttoolsIPCHandler(t, h)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, handler)
	base := "http://" + listener.Addr().String()

	healthRes, health := ipcRequest(t, http.DefaultClient, http.MethodGet, base+"/health", nil)
	if healthRes.StatusCode != 200 || health["ok"] != true || health["events"] != float64(0) {
		t.Fatalf("health = %d %#v", healthRes.StatusCode, health)
	}
	manifestRes, manifest := ipcRequest(t, http.DefaultClient, http.MethodGet, base+"/manifest", nil)
	if manifestRes.StatusCode != 200 || manifest["protocol"] != float64(2) {
		t.Fatalf("manifest = %d %#v", manifestRes.StatusCode, manifest)
	}
}

func TestIPCToolRefusalKeepsStatusAndReadableBody(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("ipc-refusal", "hi")
	server := &http.Server{Handler: newHosttoolsIPCHandler(t, h)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, server.Handler)
	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, "http://"+listener.Addr().String()+"/tool/chat_reply", map[string]any{
		"agent": h.target, "delivery_id": delivery.ID, "conversation_id": "wrong", "message": "x",
	})
	if response.StatusCode != 409 || body["is_error"] != true || !strings.Contains(body["text"].(string), "conversation_id does not match") {
		t.Fatalf("tool refusal = %d %#v", response.StatusCode, body)
	}
	if _, exists := body["status"]; exists || len(h.connector.posts) != 0 {
		t.Fatalf("transport status leaked or post occurred: %#v posts=%d", body, len(h.connector.posts))
	}
}

func TestIPCExcludesLegacyDeliveryEndpoints(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	handler := newHosttoolsIPCHandler(t, h)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, handler)
	base := "http://" + listener.Addr().String()
	for _, request := range []struct {
		method string
		path   string
		body   any
	}{
		{method: http.MethodGet, path: "/pending"},
		{method: http.MethodGet, path: "/events"},
		{method: http.MethodPost, path: "/ack", body: map[string]any{"id": 1}},
	} {
		response, _ := ipcRequest(t, http.DefaultClient, request.method, base+request.path, request.body)
		if response.StatusCode != 404 {
			t.Errorf("%s %s = %d, want 404", request.method, request.path, response.StatusCode)
		}
	}
}

func TestIPCHandledOperatorDoor(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("ipc-handled", "hi")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, "http://"+listener.Addr().String()+"/handled", map[string]any{"delivery_id": delivery.ID})
	storedEvent, _ := h.store.GetEvent(event.ID)
	if response.StatusCode != 200 || body["ok"] != true || storedEvent.HandledAt == nil {
		t.Fatalf("handled = %d %#v event=%#v", response.StatusCode, body, storedEvent)
	}
}

func TestIPCConnectorToolUsesUniformDoor(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, "http://"+listener.Addr().String()+"/tool/test_action", map[string]any{})
	if response.StatusCode != 200 || !reflect.DeepEqual(body, map[string]any{"text": "ok"}) {
		t.Fatalf("connector tool = %d %#v", response.StatusCode, body)
	}
}

func TestIPCToolErrorMapsToExact500Body(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, "http://"+listener.Addr().String()+"/tool/unknown", map[string]any{})
	want := map[string]any{"text": "error: unknown tool: unknown", "is_error": true}
	if response.StatusCode != 500 || !reflect.DeepEqual(body, want) {
		t.Fatalf("tool error = %d %#v, want %#v", response.StatusCode, body, want)
	}
}

func TestIPCHealthExactExternalKeySetAndNullMetrics(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	response, body := ipcRequest(t, http.DefaultClient, http.MethodGet, "http://"+listener.Addr().String()+"/health", nil)
	if response.StatusCode != 200 {
		t.Fatalf("health status = %d", response.StatusCode)
	}
	gotKeys := make([]string, 0, len(body))
	for key := range body {
		gotKeys = append(gotKeys, key)
	}
	sort.Strings(gotKeys)
	wantKeys := []string{
		"connectors", "draft_hold_at", "draft_hold_pane", "events", "host_tools", "ok",
		"oldest_unread_age_s", "org", "read_unconfirmed", "reconcile", "reconcile_at",
		"reconcile_source", "shadow", "target", "unposted_replies", "unread",
	}
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("health keys = %v, want %v", gotKeys, wantKeys)
	}
	for _, key := range []string{"oldest_unread_age_s", "reconcile", "reconcile_at", "reconcile_source", "draft_hold_pane", "draft_hold_at"} {
		if value, exists := body[key]; !exists || value != nil {
			t.Errorf("missing metric %s = %#v, want explicit null", key, value)
		}
	}
}

func TestListenIPCTCPRoundTrip(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	listener, err := ListenIPC(IPCListenOptions{Bind: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	response, body := ipcRequest(t, http.DefaultClient, http.MethodGet, "http://"+listener.Addr().String()+"/manifest", nil)
	if response.StatusCode != 200 || body["protocol"] != float64(2) {
		t.Fatalf("TCP manifest = %d %#v", response.StatusCode, body)
	}
}

func TestListenIPCUnixRoundTrip(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	socket := filepath.Join(t.TempDir(), "courier.sock")
	listener, err := ListenIPC(IPCListenOptions{Socket: socket})
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, newHosttoolsIPCHandler(t, h))
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	response, body := ipcRequest(t, client, http.MethodGet, "http://unix/manifest", nil)
	if response.StatusCode != 200 || body["protocol"] != float64(2) {
		t.Fatalf("unix manifest = %d %#v", response.StatusCode, body)
	}
}
