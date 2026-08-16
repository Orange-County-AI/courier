package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	kaneoTestSecret    = "a-webhook-secret"
	kaneoTestWorkspace = "ws-test-000000000000000000000000"
	kaneoTestTarget    = "agent-a"
)

type kaneoAPICall struct {
	URL    string
	Header http.Header
	Body   map[string]string
}

type kaneoAPI struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []kaneoAPICall
	status int
	body   string
}

func newKaneoAPI(t *testing.T) *kaneoAPI {
	t.Helper()
	api := &kaneoAPI{status: http.StatusOK, body: "ok"}
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(request.Body).Decode(&body)
		api.mu.Lock()
		api.calls = append(api.calls, kaneoAPICall{URL: request.URL.String(), Header: request.Header.Clone(), Body: body})
		status, responseBody := api.status, api.body
		api.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, responseBody)
	}))
	t.Cleanup(api.server.Close)
	return api
}

func (a *kaneoAPI) setResponse(status int, body string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = status
	a.body = body
}

func (a *kaneoAPI) snapshot() []kaneoAPICall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]kaneoAPICall(nil), a.calls...)
}

type kaneoRigOptions struct {
	WorkspaceID string
	BotActor    string
	Shadow      ShadowMode
	Now         func() int64
	ListenPort  int
}

type kaneoRig struct {
	store     *Store
	connector *KaneoConnector
	hostTools *HostTools
	api       *kaneoAPI
}

func newKaneoRig(t *testing.T, options kaneoRigOptions) *kaneoRig {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	api := newKaneoAPI(t)
	connector, err := NewKaneoConnector(KaneoConnectorConfig{
		Store:  store,
		Target: kaneoTestTarget,
		Options: KaneoOptions{
			Enabled:       true,
			ListenPort:    options.ListenPort,
			WebhookSecret: kaneoTestSecret,
			APIBase:       api.server.URL,
			BotKey:        "bot-key",
			WorkspaceID:   options.WorkspaceID,
			BotActor:      options.BotActor,
		},
		Shadow: options.Shadow,
		Now:    options.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(connector); err != nil {
		t.Fatal(err)
	}
	hostTools, err := NewHostTools(HostToolsOptions{
		Store: store, Connectors: registry, Shadow: options.Shadow,
		Now: func() int64 { return 1_700_000_000_000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kaneoRig{store: store, connector: connector, hostTools: hostTools, api: api}
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }

func kaneoWebhook() KaneoWebhook {
	return KaneoWebhook{
		Event:       "task.created",
		Timestamp:   "2026-01-15T00:00:00Z",
		Integration: map[string]any{"type": "generic-webhook"},
		Project:     &KaneoProject{ID: "proj", Name: "Smoke", WorkspaceID: kaneoTestWorkspace},
		Task: &KaneoTask{
			ID: "task-1", Number: int64Pointer(3), Title: "Do the thing",
			StatusName: "To Do", Priority: "high", URL: "https://x/y",
		},
		Actor: &KaneoActor{Name: stringPointer("Rachel")},
		Data:  map[string]any{"description": "please"},
	}
}

func marshalKaneoWebhook(t *testing.T, body KaneoWebhook) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func signKaneoBody(raw []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}

func callKaneoWebhook(t *testing.T, connector *KaneoConnector, raw []byte, signature string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/webhook", bytes.NewReader(raw))
	if signature != "" {
		request.Header.Set("x-kaneo-signature", signature)
	}
	response := httptest.NewRecorder()
	connector.ServeHTTP(response, request)
	return response
}

func deliverKaneoWebhook(t *testing.T, connector *KaneoConnector, body KaneoWebhook) *httptest.ResponseRecorder {
	t.Helper()
	raw := marshalKaneoWebhook(t, body)
	return callKaneoWebhook(t, connector, raw, signKaneoBody(raw, kaneoTestSecret))
}

func eventCount(t *testing.T, store *Store) int64 {
	t.Helper()
	count, err := store.CountEvents()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func TestVerifyKaneoSignatureAcceptsCorrectHMAC(t *testing.T) {
	raw := []byte(`{"event":"task.created"}`)
	if !VerifyKaneoSignature(raw, signKaneoBody(raw, kaneoTestSecret), kaneoTestSecret) {
		t.Fatal("correct HMAC was rejected")
	}
}

func TestVerifyKaneoSignatureRejectsWrongVariants(t *testing.T) {
	raw := []byte(`{"event":"task.created"}`)
	signature := signKaneoBody(raw, kaneoTestSecret)
	wrongDigest := signature[:len(signature)-1] + "0"
	if wrongDigest == signature {
		wrongDigest = signature[:len(signature)-1] + "1"
	}
	for name, test := range map[string]struct {
		raw       []byte
		signature string
		secret    string
	}{
		"digest":  {raw: raw, signature: wrongDigest, secret: kaneoTestSecret},
		"secret":  {raw: raw, signature: signKaneoBody(raw, "other"), secret: kaneoTestSecret},
		"body":    {raw: []byte(`{"event":"task.deleted"}`), signature: signature, secret: kaneoTestSecret},
		"missing": {raw: raw, signature: "", secret: kaneoTestSecret},
	} {
		t.Run(name, func(t *testing.T) {
			if VerifyKaneoSignature(test.raw, test.signature, test.secret) {
				t.Fatal("invalid HMAC was accepted")
			}
		})
	}
}

func TestVerifyKaneoSignatureWrongLengthReturnsFalse(t *testing.T) {
	if VerifyKaneoSignature([]byte("{}"), "short", kaneoTestSecret) {
		t.Fatal("wrong-length signature was accepted")
	}
}

func TestKaneoBadSignatureWritesNothing(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	response := callKaneoWebhook(t, rig.connector, marshalKaneoWebhook(t, kaneoWebhook()), "deadbeef")
	if response.Code != http.StatusForbidden || eventCount(t, rig.store) != 0 {
		t.Fatalf("response=%d events=%d", response.Code, eventCount(t, rig.store))
	}
}

func TestKaneoTaskCreatedMapsEventAndConversation(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	if response := deliverKaneoWebhook(t, rig.connector, kaneoWebhook()); response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	event, err := rig.store.GetEvent(1)
	if err != nil || event == nil {
		t.Fatalf("GetEvent() = %#v, %v", event, err)
	}
	if event.Connector != KaneoName || event.ConversationID != "kaneo:task-1" || event.User == nil || *event.User != "Rachel" || !strings.Contains(event.Content, "New Kaneo task smoke#3") {
		t.Fatalf("event = %#v", event)
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(event.MetaJSON), &meta); err != nil || meta["task_id"] != "task-1" {
		t.Fatalf("meta = %#v, %v", meta, err)
	}
	delivery, err := rig.store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil || delivery.Target != kaneoTestTarget {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
}

func TestKaneoTaskEventsShareConversation(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	bodies := []KaneoWebhook{kaneoWebhook(), kaneoWebhook(), kaneoWebhook()}
	bodies[1].Event = "task.status_changed"
	bodies[1].Data = map[string]any{"oldStatus": "To Do", "newStatus": "In Progress"}
	bodies[2].Event = "task.comment_created"
	bodies[2].Data = map[string]any{"comment": "any progress?"}
	for _, body := range bodies {
		deliverKaneoWebhook(t, rig.connector, body)
	}
	if eventCount(t, rig.store) != 3 {
		t.Fatalf("event count = %d", eventCount(t, rig.store))
	}
	for id := int64(1); id <= 3; id++ {
		event, err := rig.store.GetEvent(id)
		if err != nil || event == nil || event.ConversationID != "kaneo:task-1" {
			t.Fatalf("event %d = %#v, %v", id, event, err)
		}
	}
}

func TestKaneoDropsNonTaskEvent(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Event = "project.created"
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 0 {
		t.Fatal("non-task event was stored")
	}
}

func TestKaneoRawBodyHashDeduplicates(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	raw := marshalKaneoWebhook(t, kaneoWebhook())
	signature := signKaneoBody(raw, kaneoTestSecret)
	callKaneoWebhook(t, rig.connector, raw, signature)
	callKaneoWebhook(t, rig.connector, raw, signature)
	if eventCount(t, rig.store) != 1 {
		t.Fatalf("event count = %d", eventCount(t, rig.store))
	}
	event, err := rig.store.GetEvent(1)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if event == nil || event.EventKey != hex.EncodeToString(digest[:]) {
		t.Fatalf("event key = %#v", event)
	}
	deliveries, err := rig.store.DeliveriesForTarget(kaneoTestTarget)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, %v", deliveries, err)
	}
}

func TestKaneoDistinctChangesStayDistinct(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	first, second := kaneoWebhook(), kaneoWebhook()
	first.Event, second.Event = "task.status_changed", "task.status_changed"
	first.Data = map[string]any{"oldStatus": "To Do", "newStatus": "In Progress"}
	second.Data = map[string]any{"oldStatus": "In Progress", "newStatus": "Done"}
	deliverKaneoWebhook(t, rig.connector, first)
	deliverKaneoWebhook(t, rig.connector, second)
	if eventCount(t, rig.store) != 2 {
		t.Fatalf("event count = %d", eventCount(t, rig.store))
	}
}

func TestKaneoMalformedSignedJSONReturns400(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	raw := []byte("not json")
	response := callKaneoWebhook(t, rig.connector, raw, signKaneoBody(raw, kaneoTestSecret))
	if response.Code != http.StatusBadRequest || eventCount(t, rig.store) != 0 {
		t.Fatalf("response=%d events=%d", response.Code, eventCount(t, rig.store))
	}
}

func TestKaneoDropsEventWithoutTaskID(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Task = &KaneoTask{Number: int64Pointer(3), Title: "orphan"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 0 {
		t.Fatal("event without task id was stored")
	}
}

func TestKaneoDropsForeignWorkspaceAtIngest(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{WorkspaceID: kaneoTestWorkspace})
	body := kaneoWebhook()
	body.Project = &KaneoProject{ID: "p2", Name: "Other", WorkspaceID: "somebody-else"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 0 {
		t.Fatal("foreign workspace event was stored")
	}
}

func TestKaneoDeliversConfiguredWorkspace(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{WorkspaceID: kaneoTestWorkspace})
	deliverKaneoWebhook(t, rig.connector, kaneoWebhook())
	if eventCount(t, rig.store) != 1 {
		t.Fatal("configured workspace event was dropped")
	}
}

func TestKaneoWithoutFilterDeliversEveryWorkspace(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Project = &KaneoProject{ID: "p2", Name: "Other", WorkspaceID: "elsewhere"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 1 {
		t.Fatal("unfiltered workspace event was dropped")
	}
}

func TestCarriesKaneoBotMarkerAtLineStart(t *testing.T) {
	for _, comment := range []string{
		KaneoBotMarker + " on it",
		"some context\n" + KaneoBotMarker + " kaneo/smoke#3 done",
		"   🤖 indented still counts",
	} {
		if !CarriesKaneoBotMarker(comment) {
			t.Errorf("marker not recognized in %q", comment)
		}
	}
	for _, comment := range []string{"a human writing about 🤖 robots mid-line", ""} {
		if CarriesKaneoBotMarker(comment) {
			t.Errorf("false marker match in %q", comment)
		}
	}
}

func TestKaneoDropsMarkedBotComment(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Event = "task.comment_created"
	body.Data = map[string]any{"comment": KaneoBotMarker + " on it"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 0 {
		t.Fatal("marked bot comment was stored")
	}
}

func TestKaneoDeliversHumanComment(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Event = "task.comment_created"
	body.Data = map[string]any{"comment": "any progress?"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 1 {
		t.Fatal("human comment was dropped")
	}
}

func TestKaneoDropsConfiguredBotActor(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{BotActor: "Example Bot"})
	body := kaneoWebhook()
	body.Event = "task.comment_created"
	body.Actor = &KaneoActor{Name: stringPointer("Example Bot")}
	body.Data = map[string]any{"comment": "no marker on this one"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 0 {
		t.Fatal("configured bot actor comment was stored")
	}
}

func TestKaneoMarkerFilterAppliesOnlyToComments(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	body := kaneoWebhook()
	body.Data = map[string]any{"description": KaneoBotMarker + " filed by a worker"}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != 1 {
		t.Fatal("marked task description was incorrectly dropped")
	}
}

func TestKaneoPostReplyRouteHeadersAndPayload(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	err := rig.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "kaneo:task-1"}, "looking at it now")
	if err != nil {
		t.Fatal(err)
	}
	calls := rig.api.snapshot()
	if len(calls) != 1 || calls[0].URL != "/api/activity/comment" || calls[0].Header.Get("x-api-key") != "bot-key" || calls[0].Body["taskId"] != "task-1" {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestKaneoReplyMarkerPreventsReingestion(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	deliverKaneoWebhook(t, rig.connector, kaneoWebhook())
	before := eventCount(t, rig.store)
	if err := rig.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "kaneo:task-1"}, "looking at it now"); err != nil {
		t.Fatal(err)
	}
	comment := rig.api.snapshot()[0].Body["comment"]
	if !strings.HasPrefix(comment, KaneoBotMarker) || !CarriesKaneoBotMarker(comment) {
		t.Fatalf("comment = %q", comment)
	}
	body := kaneoWebhook()
	body.Event = "task.comment_created"
	body.Data = map[string]any{"comment": comment}
	deliverKaneoWebhook(t, rig.connector, body)
	if eventCount(t, rig.store) != before {
		t.Fatal("reply marker was reingested")
	}
}

func TestKaneoExistingMarkerIsNotDuplicated(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	message := KaneoBotMarker + " already marked"
	if err := rig.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "kaneo:task-1"}, message); err != nil {
		t.Fatal(err)
	}
	if got := rig.api.snapshot()[0].Body["comment"]; got != message {
		t.Fatalf("comment = %q, want %q", got, message)
	}
}

func TestKaneoPostReplyRequiresConfirmedResponse(t *testing.T) {
	withEvent := func(rig *kaneoRig) (*Event, *Delivery) {
		t.Helper()
		deliverKaneoWebhook(t, rig.connector, kaneoWebhook())
		event, err := rig.store.GetEvent(1)
		if err != nil || event == nil {
			t.Fatalf("GetEvent() = %#v, %v", event, err)
		}
		delivery, err := rig.store.OpenDeliveryForEvent(event.ID)
		if err != nil || delivery == nil {
			t.Fatalf("OpenDeliveryForEvent() = %#v, %v", delivery, err)
		}
		return event, delivery
	}
	reply := func(rig *kaneoRig, deliveryID string) ToolResult {
		t.Helper()
		result, err := rig.hostTools.ChatReply(context.Background(), map[string]any{
			"agent": kaneoTestTarget, "delivery_id": deliveryID,
			"conversation_id": "kaneo:task-1", "message": "done",
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	confirmed := newKaneoRig(t, kaneoRigOptions{})
	confirmedEvent, confirmedDelivery := withEvent(confirmed)
	if result := reply(confirmed, confirmedDelivery.ID); result.IsError {
		t.Fatalf("confirmed reply = %#v", result)
	}
	confirmedEvent, err := confirmed.store.GetEvent(confirmedEvent.ID)
	if err != nil || confirmedEvent.HandledAt == nil {
		t.Fatalf("confirmed event = %#v, %v", confirmedEvent, err)
	}

	rejected := newKaneoRig(t, kaneoRigOptions{})
	rejectedEvent, rejectedDelivery := withEvent(rejected)
	rejected.api.setResponse(http.StatusUnauthorized, "nope")
	if result := reply(rejected, rejectedDelivery.ID); !result.IsError || !strings.Contains(result.Text, "NOT yet posted") {
		t.Fatalf("rejected reply = %#v", result)
	}
	rejectedEvent, err = rejected.store.GetEvent(rejectedEvent.ID)
	if err != nil || rejectedEvent.HandledAt != nil {
		t.Fatalf("rejected event = %#v, %v", rejectedEvent, err)
	}
	rejectedDelivery, err = rejected.store.GetDelivery(rejectedDelivery.ID)
	if err != nil || rejectedDelivery.Status != DeliveryReplied {
		t.Fatalf("rejected delivery = %#v, %v", rejectedDelivery, err)
	}
}

func TestKaneoCommentToolPostsAnyTask(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	result, err := rig.connector.CallTool(context.Background(), "kaneo_comment", map[string]any{"task_id": "other-task", "message": "a note"})
	if err != nil || result.IsError {
		t.Fatalf("CallTool() = %#v, %v", result, err)
	}
	calls := rig.api.snapshot()
	if len(calls) != 1 || calls[0].URL != "/api/activity/comment" || calls[0].Body["taskId"] != "other-task" {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestKaneoCommentToolRejectsMissingArguments(t *testing.T) {
	rig := newKaneoRig(t, kaneoRigOptions{})
	for _, args := range []map[string]any{{"task_id": "t"}, {"message": "x"}} {
		result, err := rig.connector.CallTool(context.Background(), "kaneo_comment", args)
		if err != nil || !result.IsError || result.Status != http.StatusBadRequest {
			t.Fatalf("CallTool(%#v) = %#v, %v", args, result, err)
		}
	}
	if calls := rig.api.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestKaneoIngestFailureReturns500WithoutRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	api := newKaneoAPI(t)
	connector, err := NewKaneoConnector(KaneoConnectorConfig{
		Store: store, Target: kaneoTestTarget,
		Options: KaneoOptions{Enabled: true, WebhookSecret: kaneoTestSecret, APIBase: api.server.URL, BotKey: "bot-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	response := deliverKaneoWebhook(t, connector, kaneoWebhook())
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if eventCount(t, reopened) != 0 {
		t.Fatal("failed ingest persisted a row")
	}
}

func TestKaneoStopDrainsInflightWebhook(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	rig := newKaneoRig(t, kaneoRigOptions{
		ListenPort: 0,
		Now: func() int64 {
			once.Do(func() { close(entered) })
			<-release
			return 1_700_000_000_000
		},
	})
	if err := rig.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rig.connector.Stop(ctx)
	})
	if host, _, found := strings.Cut(rig.connector.Address(), ":"); !found || host != "127.0.0.1" {
		t.Fatalf("listener address = %q", rig.connector.Address())
	}
	raw := marshalKaneoWebhook(t, kaneoWebhook())
	request, err := http.NewRequest(http.MethodPost, "http://"+rig.connector.Address()+"/webhook", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("x-kaneo-signature", signKaneoBody(raw, kaneoTestSecret))
	responseResult := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		responseResult <- struct {
			response *http.Response
			err      error
		}{response: response, err: requestErr}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook never reached persistence")
	}
	stopResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopResult <- rig.connector.Stop(ctx)
	}()
	select {
	case err := <-stopResult:
		t.Fatalf("Stop returned before in-flight persistence completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	result := <-responseResult
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.response.Body.Close()
	if result.response.StatusCode != http.StatusOK || eventCount(t, rig.store) != 1 {
		t.Fatalf("response=%d events=%d", result.response.StatusCode, eventCount(t, rig.store))
	}
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}
}
