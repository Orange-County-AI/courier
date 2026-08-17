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
		"reconcile_source", "shadow", "paused", "target", "unposted_replies", "unread",
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

// ipcPluginHandler wires the three plugin routes onto the hosttools harness with
// recording closures, so the tests assert the HTTP contract the pane depends on
// rather than the daemon's internals.
type ipcPluginRecorder struct {
	inbox     InboxState
	inboxErr  error
	kick      KickResult
	kicks     int
	paused    bool
	pauseSets []bool
	resumes   int
}

func ipcPluginHandler(t *testing.T, h *hosttoolsHarness, rec *ipcPluginRecorder) http.Handler {
	t.Helper()
	handler, err := NewIPCHandler(IPCOptions{
		Store:    h.store,
		Manifest: func() (Manifest, error) { return Manifest{Name: "courier-test"}, nil },
		CallTool: func(context.Context, string, map[string]any) (ToolResult, error) {
			return ToolResult{}, errors.New("no tools in this harness")
		},
		Health: func() HealthState {
			return HealthState{Org: "test-org", Target: h.target, Paused: rec.paused}
		},
		Inbox: func(context.Context) (InboxState, error) {
			if rec.inboxErr != nil {
				return InboxState{}, rec.inboxErr
			}
			state := rec.inbox
			state.Paused = rec.paused
			return state, nil
		},
		Kick: func(context.Context) (KickResult, error) {
			rec.kicks++
			return rec.kick, nil
		},
		Pause: func(_ context.Context, paused bool) (bool, error) {
			rec.pauseSets = append(rec.pauseSets, paused)
			rec.paused = paused
			if !paused {
				rec.resumes++
			}
			return rec.paused, nil
		},
		Now: func() int64 { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func ipcPluginBase(t *testing.T, h *hosttoolsHarness, rec *ipcPluginRecorder) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	startIPCServer(t, listener, ipcPluginHandler(t, h, rec))
	return "http://" + listener.Addr().String()
}

func TestIPCInboxEchoesOpenQueueHoldAndPause(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	rec := &ipcPluginRecorder{
		paused: true,
		inbox: InboxState{
			Target:    h.target,
			DraftHold: &DraftHold{PaneID: "w7V:p1", Agent: "omp", At: 1786943678814},
			Rows: []InboxRow{{
				DeliveryID: "d-1", EventID: 12, Status: "pending", Connector: "mattermost",
				ConversationID: "channel-7:thread-9", User: "Dana", AttemptCount: 0,
				Read: false, CreatedAt: 1786940000000, Preview: "Can you check the batch",
			}},
		},
	}
	response, body := ipcRequest(t, http.DefaultClient, http.MethodGet, ipcPluginBase(t, h, rec)+"/inbox", nil)
	if response.StatusCode != 200 || body["ok"] != true || body["target"] != h.target || body["paused"] != true {
		t.Fatalf("inbox = %d %#v", response.StatusCode, body)
	}
	hold, ok := body["draft_hold"].(map[string]any)
	if !ok || hold["pane_id"] != "w7V:p1" || hold["agent"] != "omp" {
		t.Fatalf("draft_hold = %#v", body["draft_hold"])
	}
	rows, ok := body["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v", body["rows"])
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("row = %#v", rows[0])
	}
	for key, want := range map[string]any{
		"delivery_id": "d-1", "event_id": float64(12), "status": "pending",
		"connector": "mattermost", "conversation_id": "channel-7:thread-9", "user": "Dana",
		"attempt_count": float64(0), "read": false, "created_at": float64(1786940000000),
		"preview": "Can you check the batch", "last_error": nil,
	} {
		if row[key] != want {
			t.Errorf("row[%s] = %#v, want %#v", key, row[key], want)
		}
	}
}

func TestIPCInboxSerializesAnEmptyQueueAsAList(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	rec := &ipcPluginRecorder{inbox: InboxState{Target: h.target}}
	_, body := ipcRequest(t, http.DefaultClient, http.MethodGet, ipcPluginBase(t, h, rec)+"/inbox", nil)
	rows, ok := body["rows"].([]any)
	if !ok || len(rows) != 0 {
		t.Fatalf("rows = %#v, want an empty list rather than null", body["rows"])
	}
	if body["draft_hold"] != nil {
		t.Fatalf("draft_hold = %#v, want explicit null", body["draft_hold"])
	}
}

func TestIPCKickReportsOutcomesAndBusy(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	rec := &ipcPluginRecorder{kick: KickResult{Outcomes: 1}}
	base := ipcPluginBase(t, h, rec)

	// No body at all: a kick carries no arguments.
	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/kick", nil)
	if response.StatusCode != 200 || body["ok"] != true || body["busy"] != false || body["outcomes"] != float64(1) {
		t.Fatalf("kick = %d %#v", response.StatusCode, body)
	}
	if rec.kicks != 1 {
		t.Fatalf("tick closure calls = %d, want 1", rec.kicks)
	}

	rec.kick = KickResult{Busy: true}
	_, body = ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/kick", nil)
	if body["ok"] != true || body["busy"] != true || body["outcomes"] != float64(0) {
		t.Fatalf("busy kick = %#v", body)
	}
}

func TestIPCPauseFlipsStateAndRejectsNonBooleans(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	rec := &ipcPluginRecorder{}
	base := ipcPluginBase(t, h, rec)

	response, body := ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/pause", map[string]any{"paused": true})
	if response.StatusCode != 200 || body["ok"] != true || body["paused"] != true {
		t.Fatalf("pause = %d %#v", response.StatusCode, body)
	}
	if _, health := ipcRequest(t, http.DefaultClient, http.MethodGet, base+"/health", nil); health["paused"] != true {
		t.Fatalf("health paused = %#v", health["paused"])
	}

	response, body = ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/pause", map[string]any{"paused": "yes"})
	if response.StatusCode != 400 || body["error"] != "paused must be a boolean" {
		t.Fatalf("non-boolean pause = %d %#v", response.StatusCode, body)
	}
	response, body = ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/pause", map[string]any{})
	if response.StatusCode != 400 || body["error"] != "paused must be a boolean" {
		t.Fatalf("missing pause = %d %#v", response.StatusCode, body)
	}

	// Resume must deliver now rather than at the next tick.
	if _, body = ipcRequest(t, http.DefaultClient, http.MethodPost, base+"/pause", map[string]any{"paused": false}); body["paused"] != false {
		t.Fatalf("resume = %#v", body)
	}
	if !reflect.DeepEqual(rec.pauseSets, []bool{true, false}) || rec.resumes != 1 {
		t.Fatalf("pause calls = %#v resumes = %d", rec.pauseSets, rec.resumes)
	}
}
