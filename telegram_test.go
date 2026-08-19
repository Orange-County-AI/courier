package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const telegramTestToken = "123456:super-secret-token"

type telegramStubCall struct {
	Method string
	Body   map[string]any
}

type telegramAPIStub struct {
	mu sync.Mutex

	calls       []telegramStubCall
	fileContent []byte
	failSend    bool
	failGetFile bool
	sendStarted chan struct{}
	sendRelease <-chan struct{}
}

func (s *telegramAPIStub) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, "/file/bot"+telegramTestToken+"/") {
		s.mu.Lock()
		content := append([]byte(nil), s.fileContent...)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
		return
	}
	prefix := "/bot" + telegramTestToken + "/"
	if !strings.HasPrefix(request.URL.Path, prefix) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	method := strings.TrimPrefix(request.URL.Path, prefix)
	body := make(map[string]any)
	if request.Body != nil {
		_ = json.NewDecoder(request.Body).Decode(&body)
	}
	s.mu.Lock()
	s.calls = append(s.calls, telegramStubCall{Method: method, Body: body})
	failSend := s.failSend
	failGetFile := s.failGetFile
	started := s.sendStarted
	release := s.sendRelease
	s.mu.Unlock()

	if method == "sendMessage" && started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if method == "sendMessage" && release != nil {
		select {
		case <-release:
		case <-request.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case method == "sendMessage" && failSend:
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":500,"description":"upstream rejected token %s"}`, telegramTestToken)
	case method == "getFile" && failGetFile:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":400,"description":"bad token %s"}`, telegramTestToken)
	case method == "sendMessage":
		_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":321}}`)
	case method == "getFile":
		_, _ = io.WriteString(w, `{"ok":true,"result":{"file_id":"photo-file","file_unique_id":"unique-photo","file_path":"photos/photo.jpg"}}`)
	default:
		_, _ = io.WriteString(w, `{"ok":true,"result":true}`)
	}
}

func (s *telegramAPIStub) Calls() []telegramStubCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]telegramStubCall(nil), s.calls...)
}

func (s *telegramAPIStub) SetFailSend(value bool) {
	s.mu.Lock()
	s.failSend = value
	s.mu.Unlock()
}

func (s *telegramAPIStub) SetFailGetFile(value bool) {
	s.mu.Lock()
	s.failGetFile = value
	s.mu.Unlock()
}

type telegramHarness struct {
	store     *Store
	connector *TelegramConnector
	stub      *telegramAPIStub
	server    *httptest.Server
	opts      TelegramOptions
	logs      *bytes.Buffer
	target    string
}

func newTelegramHarness(t *testing.T, shadow bool) *telegramHarness {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "courier.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	stub := &telegramAPIStub{fileContent: []byte("telegram attachment")}
	server := httptest.NewServer(stub)
	attachmentDir := filepath.Join(t.TempDir(), "attachments")
	client, err := NewTelegramClient(TelegramClientConfig{
		Token: telegramTestToken, BaseURL: server.URL, AttachmentDir: attachmentDir, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := TelegramOptions{
		Enabled: true, ListenPort: 7784, BotToken: telegramTestToken,
		WebhookSecret: "webhook-secret", BotUsername: "example_bot",
		BotUserID: "999", AllowedUserIDs: "42", AllowedChatIDs: "-1001",
		AttachmentDir: attachmentDir, GroupRequireMention: true,
		ClearDisabled: true, ClearAck: "clear is disabled",
		DisconnectNotice: "helper is unavailable; your message is queued",
	}
	logs := &bytes.Buffer{}
	connector, err := NewTelegramConnector(TelegramConnectorConfig{
		Store: store, Target: "helper", Options: opts, Shadow: NewShadowMode(shadow), Client: client,
		Now:           func() int64 { return 1_700_000_000_000 },
		Log:           func(format string, args ...any) { fmt.Fprintf(logs, format+"\n", args...) },
		TypingRefresh: time.Hour, TypingMaximum: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	harness := &telegramHarness{
		store: store, connector: connector, stub: stub, server: server,
		opts: opts, logs: logs, target: "helper",
	}
	t.Cleanup(func() {
		connector.stopAllTyping()
		server.Close()
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return harness
}

func telegramDMUpdate(updateID, messageID int64, from TelegramUser, text string) TelegramUpdate {
	return TelegramUpdate{UpdateID: updateID, Message: &TelegramMessage{
		MessageID: messageID, Date: 1_700_000_000, From: &from,
		Chat: TelegramChat{ID: 42, Type: "private"}, Text: text,
	}}
}

func postTelegramWebhook(t *testing.T, connector *TelegramConnector, update TelegramUpdate) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(raw))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
	response := httptest.NewRecorder()
	connector.ServeHTTP(response, request)
	return response
}

func TestTelegramWebhookQueuesStableEventAndDeduplicates(t *testing.T) {
	h := newTelegramHarness(t, false)
	update := telegramDMUpdate(700, 12, TelegramUser{ID: 42, FirstName: "Ada", Username: "ada"}, "hello helper")
	if response := postTelegramWebhook(t, h.connector, update); response.Code != http.StatusOK {
		t.Fatalf("webhook status = %d, body=%s", response.Code, response.Body.String())
	}
	event, err := h.store.FindEvent(TelegramName, "update:700")
	if err != nil || event == nil {
		t.Fatalf("FindEvent = %#v, %v", event, err)
	}
	if event.Connector != TelegramName || event.EventKey != "update:700" || event.ConversationID != "42" || event.Content != "hello helper" || event.User == nil || *event.User != "ada" {
		t.Fatalf("event = %#v", event)
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(event.MetaJSON), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["message_id"] != "12" || meta["update_id"] != "700" || meta["chat_type"] != "private" {
		t.Fatalf("meta = %#v", meta)
	}
	if delivery, err := h.store.OpenDeliveryForEvent(event.ID); err != nil || delivery == nil || delivery.Target != "helper" {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
	if response := postTelegramWebhook(t, h.connector, update); response.Code != http.StatusOK {
		t.Fatalf("duplicate webhook status = %d", response.Code)
	}
	if count, err := h.store.CountEvents(TelegramName); err != nil || count != 1 {
		t.Fatalf("event count = %d, %v", count, err)
	}
	if watermark, err := h.store.GetSyncState(telegramWatermarkKey); err != nil || watermark == nil || *watermark != 700 {
		t.Fatalf("watermark = %v, %v", watermark, err)
	}
}

func TestTelegramForumTopicConversationRoundTrips(t *testing.T) {
	h := newTelegramHarness(t, false)
	update := TelegramUpdate{UpdateID: 750, Message: &TelegramMessage{
		MessageID: 15, MessageThreadID: 77, Date: 1_700_000_000,
		From: &TelegramUser{ID: 42, Username: "ada"},
		Chat: TelegramChat{ID: -1001, Type: "supergroup", Title: "Operators"},
		Text: "@example_bot investigate",
	}}
	event, err := h.connector.Ingest(context.Background(), update, nil)
	if err != nil || event == nil {
		t.Fatalf("Ingest = %#v, %v", event, err)
	}
	if event.ConversationID != "-1001:77" || event.Content != "investigate" {
		t.Fatalf("event = %#v", event)
	}
	chatID, threadID, err := TelegramDecomposeConversationID(event.ConversationID)
	if err != nil || chatID != "-1001" || threadID != 77 {
		t.Fatalf("decompose = %q, %d, %v", chatID, threadID, err)
	}
}

func TestTelegramOwnMessageIsNeverIngested(t *testing.T) {
	h := newTelegramHarness(t, false)
	update := telegramDMUpdate(701, 13, TelegramUser{ID: 999, IsBot: true, Username: "example_bot"}, "loop")
	if _, err := h.connector.Ingest(context.Background(), update, nil); err != nil {
		t.Fatal(err)
	}
	if count, err := h.store.CountEvents(TelegramName); err != nil || count != 0 {
		t.Fatalf("event count = %d, %v", count, err)
	}
	if len(h.stub.Calls()) != 0 {
		t.Fatalf("own message caused Telegram calls: %#v", h.stub.Calls())
	}
}

func TestTelegramWatermarkAndDedupeSurviveConnectorRestart(t *testing.T) {
	h := newTelegramHarness(t, false)
	first := telegramDMUpdate(800, 20, TelegramUser{ID: 42, Username: "ada"}, "first")
	if _, err := h.connector.Ingest(context.Background(), first, nil); err != nil {
		t.Fatal(err)
	}
	h.connector.stopAllTyping()
	client, err := NewTelegramClient(TelegramClientConfig{
		Token: telegramTestToken, BaseURL: h.server.URL, AttachmentDir: h.opts.AttachmentDir, HTTPClient: h.server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewTelegramConnector(TelegramConnectorConfig{
		Store: h.store, Target: h.target, Options: h.opts, Client: client,
		TypingRefresh: time.Hour, TypingMaximum: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restarted.stopAllTyping)
	if duplicate, err := restarted.Ingest(context.Background(), first, nil); err != nil || duplicate != nil {
		t.Fatalf("replayed update = %#v, %v", duplicate, err)
	}
	second := telegramDMUpdate(801, 21, TelegramUser{ID: 42, Username: "ada"}, "second")
	if event, err := restarted.Ingest(context.Background(), second, nil); err != nil || event == nil {
		t.Fatalf("new update = %#v, %v", event, err)
	}
	if count, _ := h.store.CountEvents(TelegramName); count != 2 {
		t.Fatalf("event count = %d", count)
	}
	if watermark, err := h.store.GetSyncState(telegramWatermarkKey); err != nil || watermark == nil || *watermark != 801 {
		t.Fatalf("watermark = %v, %v", watermark, err)
	}
}

func TestTelegramPostReplyWaitsForConfirmationAndRoutesTopic(t *testing.T) {
	h := newTelegramHarness(t, false)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h.stub.mu.Lock()
	h.stub.sendStarted = started
	h.stub.sendRelease = release
	h.stub.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		result <- h.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "-1001:77"}, "confirmed reply")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("sendMessage did not start")
	}
	select {
	case err := <-result:
		t.Fatalf("PostReply returned before Telegram confirmation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	calls := h.stub.Calls()
	var send *telegramStubCall
	for i := range calls {
		if calls[i].Method == "sendMessage" {
			send = &calls[i]
		}
	}
	if send == nil || send.Body["chat_id"] != "-1001" || send.Body["message_thread_id"] != float64(77) || send.Body["text"] != "confirmed reply" {
		t.Fatalf("send call = %#v; all=%#v", send, calls)
	}
}

func TestTelegramFailedSendRemainsUnsettled(t *testing.T) {
	h := newTelegramHarness(t, false)
	h.stub.SetFailSend(true)
	now := int64(1_700_000_000_000)
	user := "ada"
	event, err := h.store.InsertEvent(EventInsert{
		Connector: TelegramName, EventKey: "failure", ConversationID: "42", User: &user,
		Content: "question", MetaJSON: `{}`, RawJSON: `{}`,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := h.store.InsertDelivery(event.ID, h.target, now)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(h.connector); err != nil {
		t.Fatal(err)
	}
	tools, err := NewHostTools(HostToolsOptions{Store: h.store, Connectors: registry, Now: func() int64 { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tools.ChatReply(context.Background(), map[string]any{
		"agent": h.target, "delivery_id": delivery.ID, "conversation_id": "42", "message": "answer",
	})
	if err != nil {
		t.Fatal(err)
	}
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
	if !result.IsError || !strings.Contains(result.Text, "NOT yet posted") || storedDelivery.Status != DeliveryReplied || storedEvent.HandledAt != nil || reply == nil || reply.PostedAt != nil || reply.PostError == nil {
		t.Fatalf("failure settled: result=%#v delivery=%#v event=%#v reply=%#v", result, storedDelivery, storedEvent, reply)
	}
}

func TestTelegramShadowSuppressesEveryOutboundEffect(t *testing.T) {
	h := newTelegramHarness(t, true)
	update := telegramDMUpdate(900, 30, TelegramUser{ID: 42, Username: "ada"}, "photo")
	update.Message.Photo = []TelegramPhoto{{FileID: "photo-file", FileUniqueID: "photo-unique", FileSize: 100}}
	if event, err := h.connector.Ingest(context.Background(), update, nil); err != nil || event == nil {
		t.Fatalf("shadow ingest = %#v, %v", event, err)
	}
	if err := h.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "42"}, "no send"); err == nil || !strings.Contains(err.Error(), ShadowRefusal) {
		t.Fatalf("shadow PostReply error = %v", err)
	}
	tool, err := h.connector.CallTool(context.Background(), "telegram_react", map[string]any{
		"conversation_id": "42", "message_id": "30", "emoji": "👍",
	})
	if err != nil || !tool.IsError || !strings.Contains(tool.Text, ShadowRefusal) {
		t.Fatalf("shadow tool = %#v, %v", tool, err)
	}
	clear := telegramDMUpdate(901, 31, TelegramUser{ID: 42, Username: "ada"}, "/clear")
	if _, err := h.connector.Ingest(context.Background(), clear, nil); err != nil {
		t.Fatal(err)
	}
	if calls := h.stub.Calls(); len(calls) != 0 {
		t.Fatalf("shadow made outbound calls: %#v", calls)
	}
}

func TestTelegramAttachmentIsDownloadedAndStoredAsLocalPath(t *testing.T) {
	h := newTelegramHarness(t, false)
	update := telegramDMUpdate(950, 40, TelegramUser{ID: 42, Username: "ada"}, "look")
	update.Message.Photo = []TelegramPhoto{
		{FileID: "small", FileUniqueID: "small", FileSize: 10, Width: 10, Height: 10},
		{FileID: "photo-file", FileUniqueID: "large", FileSize: 100, Width: 100, Height: 100},
	}
	event, err := h.connector.Ingest(context.Background(), update, nil)
	if err != nil || event == nil {
		t.Fatalf("Ingest = %#v, %v", event, err)
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(event.MetaJSON), &meta); err != nil {
		t.Fatal(err)
	}
	path := meta["attachment_path"]
	if path == "" || !filepath.IsAbs(path) || !strings.Contains(event.Content, path) {
		t.Fatalf("attachment path=%q content=%q meta=%#v", path, event.Content, meta)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "telegram attachment" {
		t.Fatalf("attachment bytes=%q, %v", contents, err)
	}
}

func TestTelegramUnavailableDispatchSendsNoticeOnceAndKeepsQueued(t *testing.T) {
	h := newTelegramHarness(t, false)
	now := int64(1_700_000_000_000)
	user := "ada"
	event, err := h.store.InsertEvent(EventInsert{
		Connector: TelegramName, EventKey: "unavailable", ConversationID: "-1001:77", User: &user,
		Content: "question", MetaJSON: `{"message_id":"55"}`, RawJSON: `{}`,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := h.store.InsertDelivery(event.ID, h.target, now)
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, paneID := "w1", "w1:p1"
	source, kind, session := "herdr:omp", "id", "session-1"
	if _, err := h.store.PutReconcilerState(ReconcilerStateInput{
		OrgID: "demo", WorkspaceID: &workspaceID, PaneID: &paneID, PaneLabel: h.target,
		AgentKind: "omp", NativeSessionSource: &source, NativeSessionKind: &kind, NativeSessionValue: &session,
	}, now); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(h.connector); err != nil {
		t.Fatal(err)
	}
	driver := &FakeDriver{
		PromptResults: []PromptResult{
			{Code: "agent_not_found", Error: "helper is unavailable"},
			{Code: "agent_not_found", Error: "helper is still unavailable"},
		},
		StartErr: fmt.Errorf("pane unavailable"),
	}
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Store: h.store, Driver: driver, Target: h.target, OrgID: "demo",
		Now: func() int64 { return now }, Connectors: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		outcomes, err := dispatcher.Drain(context.Background())
		if err != nil || len(outcomes) != 1 || outcomes[0].OK {
			t.Fatalf("Drain attempt %d = %#v, %v", attempt, outcomes, err)
		}
	}
	var sends []telegramStubCall
	for _, call := range h.stub.Calls() {
		if call.Method == "sendMessage" {
			sends = append(sends, call)
		}
	}
	if len(sends) != 1 {
		t.Fatalf("disconnect notice sends = %#v", sends)
	}
	reply, _ := sends[0].Body["reply_parameters"].(map[string]any)
	if sends[0].Body["chat_id"] != "-1001" ||
		sends[0].Body["message_thread_id"] != float64(77) ||
		sends[0].Body["text"] != h.opts.DisconnectNotice ||
		reply["message_id"] != float64(55) {
		t.Fatalf("disconnect notice = %#v", sends[0])
	}
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	if storedDelivery.Status != DeliveryPending || storedDelivery.AttemptCount != 2 || storedEvent.HandledAt != nil {
		t.Fatalf("notice settled delivery: delivery=%#v event=%#v", storedDelivery, storedEvent)
	}
}

func TestTelegramTokenNeverAppearsInErrorsOrLogs(t *testing.T) {
	h := newTelegramHarness(t, false)
	h.stub.SetFailSend(true)
	err := h.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "42"}, "fail")
	if err == nil || strings.Contains(err.Error(), telegramTestToken) || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("send error = %v", err)
	}
	h.stub.SetFailGetFile(true)
	update := telegramDMUpdate(980, 50, TelegramUser{ID: 42, Username: "ada"}, "photo")
	update.Message.Photo = []TelegramPhoto{{FileID: "photo-file", FileUniqueID: "photo", FileSize: 10}}
	if event, err := h.connector.Ingest(context.Background(), update, nil); err != nil || event == nil {
		t.Fatalf("Ingest after attachment failure = %#v, %v", event, err)
	}
	if strings.Contains(h.logs.String(), telegramTestToken) {
		t.Fatalf("token leaked into logs: %s", h.logs.String())
	}
}

func TestTelegramProductionEnvironmentAndRegistration(t *testing.T) {
	values := map[string]string{
		"CHANNEL_ORG": "demo", "CHANNEL_TARGET": "helper", "CHANNEL_CONNECTORS": "telegram",
		"TELEGRAM_LISTEN_PORT": "7784", "TELEGRAM_BOT_TOKEN": telegramTestToken,
		"TELEGRAM_WEBHOOK_SECRET": "webhook-secret", "TELEGRAM_BOT_USERNAME": "example_bot",
		"TELEGRAM_GROUP_REQUIRE_MENTION": "1", "TELEGRAM_REQUIRE_VISIBLE_ACK": "1", "TELEGRAM_CLEAR_DISABLED": "1",
		"TELEGRAM_CLEAR_ACK": "clear is disabled", "TELEGRAM_DISCONNECT_NOTICE": "helper is unavailable",
		"TELEGRAM_ALLOWED_USER_IDS": "42", "TELEGRAM_ALLOWED_CHAT_IDS": "-1001",
		"TELEGRAM_ATTACHMENT_DIR": "/var/lib/courier/telegram-attachments",
	}
	lookup := func(name string) (string, bool) { value, ok := values[name]; return value, ok }
	opts, err := serveOptionsFromEnv(lookup, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Telegram.Enabled || opts.Telegram.ListenPort != 7784 || opts.Telegram.BotToken != telegramTestToken || !opts.Telegram.GroupRequireMention || !opts.Telegram.RequireVisibleAck || !opts.Telegram.ClearDisabled || opts.Telegram.AttachmentDir != "/var/lib/courier/telegram-attachments" || opts.Telegram.DisconnectNotice != "helper is unavailable" {
		t.Fatalf("telegram options = %#v", opts.Telegram)
	}
	store, err := Open(filepath.Join(t.TempDir(), "courier.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry, active, err := serveBuildConnectors(store, opts, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	if registry.Get(TelegramName) == nil || len(active) != 1 || active[0].Name() != TelegramName {
		t.Fatalf("telegram registration: active=%#v connector=%#v", active, registry.Get(TelegramName))
	}
	if instructions := registry.Get(TelegramName).Instructions(); !strings.Contains(instructions, "visible acknowledgement") || !strings.Contains(instructions, "do not silently settle") {
		t.Fatalf("telegram visible acknowledgement instructions = %q", instructions)
	}
}
