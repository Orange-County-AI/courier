package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	gmailTestAccount = "gateway@example.com"
	gmailTestTarget  = "agent-a"
)

type gmailTestSent struct {
	Raw      string
	ThreadID string
}

type gmailTestHistoryResult struct {
	IDs       []string
	HistoryID string
	NextToken string
	Err       error
}

type gmailFakeAPI struct {
	mu sync.Mutex

	historyID    string
	messages     map[string]GmailMessage
	history      []gmailTestHistoryResult
	threads      map[string]GmailThreadMetadata
	attachments  map[string]string
	sent         []gmailTestSent
	sendFails    error
	profileCalls int
	server       *httptest.Server
}

func gmailNewFakeAPI() *gmailFakeAPI {
	return &gmailFakeAPI{
		historyID: "1000", messages: make(map[string]GmailMessage),
		threads: make(map[string]GmailThreadMetadata), attachments: make(map[string]string),
	}
}
func (f *gmailFakeAPI) gmailStartServer(t *testing.T) {
	t.Helper()
	f.server = httptest.NewServer(f)
	t.Cleanup(f.server.Close)
}

func (f *gmailFakeAPI) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/profile":
		profile, err := f.GetProfile(request.Context())
		f.gmailWriteResponse(w, profile, err)
	case request.Method == http.MethodGet && request.URL.Path == "/history":
		page, err := f.HistoryList(request.Context(), request.URL.Query().Get("startHistoryId"), request.URL.Query().Get("pageToken"))
		f.gmailWriteResponse(w, page, err)
	case request.Method == http.MethodPost && request.URL.Path == "/messages/send":
		var body struct {
			Raw      string `json:"raw"`
			ThreadID string `json:"threadId"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		sent, err := f.Send(request.Context(), body.Raw, body.ThreadID)
		f.gmailWriteResponse(w, sent, err)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/messages/"):
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/messages/"), "/")
		if len(parts) == 3 && parts[1] == "attachments" {
			size, data, err := f.GetAttachment(request.Context(), parts[0], parts[2])
			f.gmailWriteResponse(w, map[string]any{"size": size, "data": data}, err)
			return
		}
		message, err := f.GetMessage(request.Context(), parts[0], request.URL.Query().Get("format"))
		f.gmailWriteResponse(w, message, err)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/threads/"):
		threadID := strings.TrimPrefix(request.URL.Path, "/threads/")
		thread, err := f.GetThreadMetadata(request.Context(), threadID)
		f.gmailWriteResponse(w, thread, err)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (f *gmailFakeAPI) gmailWriteResponse(w http.ResponseWriter, value any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		if httpError, ok := err.(*GmailHTTPError); ok {
			status = httpError.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (f *gmailFakeAPI) GetProfile(context.Context) (GmailProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.profileCalls++
	return GmailProfile{EmailAddress: gmailTestAccount, MessagesTotal: 1, ThreadsTotal: 1, HistoryID: f.historyID}, nil
}

func (f *gmailFakeAPI) HistoryList(context.Context, string, string) (GmailHistoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.history) == 0 {
		return GmailHistoryPage{HistoryID: f.historyID}, nil
	}
	result := f.history[0]
	f.history = f.history[1:]
	if result.Err != nil {
		return GmailHistoryPage{}, result.Err
	}
	historyID := result.HistoryID
	if historyID == "" {
		historyID = f.historyID
	}
	records := make([]GmailHistoryRecord, 0, len(result.IDs))
	for _, id := range result.IDs {
		records = append(records, GmailHistoryRecord{ID: "1", MessagesAdded: []GmailHistoryAdded{{Message: GmailHistoryMessage{ID: id, ThreadID: id}}}})
	}
	return GmailHistoryPage{History: records, HistoryID: historyID, NextPageToken: result.NextToken}, nil
}

func (f *gmailFakeAPI) GetMessage(_ context.Context, id, _ string) (GmailMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	message, ok := f.messages[id]
	if !ok {
		return GmailMessage{}, &GmailHTTPError{Status: 404, Message: "not found"}
	}
	return message, nil
}

func (f *gmailFakeAPI) GetAttachment(_ context.Context, messageID, attachmentID string) (int64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.attachments[messageID+":"+attachmentID]
	if !ok {
		return 0, "", fmt.Errorf("attachment not found")
	}
	return int64(len(data)), data, nil
}

func (f *gmailFakeAPI) GetThreadMetadata(_ context.Context, threadID string) (GmailThreadMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	thread, ok := f.threads[threadID]
	if !ok {
		return GmailThreadMetadata{}, fmt.Errorf("no thread %s", threadID)
	}
	return thread, nil
}

func (f *gmailFakeAPI) Send(_ context.Context, raw, threadID string) (GmailSentMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendFails != nil {
		return GmailSentMessage{}, f.sendFails
	}
	f.sent = append(f.sent, gmailTestSent{Raw: raw, ThreadID: threadID})
	id := fmt.Sprintf("sent-%d", len(f.sent))
	if threadID == "" {
		threadID = fmt.Sprintf("thread-%d", len(f.sent))
	}
	return GmailSentMessage{ID: id, ThreadID: threadID}, nil
}

func (f *gmailFakeAPI) gmailAddMessage(message GmailMessage) {
	f.mu.Lock()
	f.messages[message.ID] = message
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailQueueHistory(results ...gmailTestHistoryResult) {
	f.mu.Lock()
	f.history = append(f.history, results...)
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailSetHistoryID(historyID string) {
	f.mu.Lock()
	f.historyID = historyID
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailSetThread(thread GmailThreadMetadata) {
	f.mu.Lock()
	f.threads[thread.ID] = thread
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailSetAttachment(messageID, attachmentID string, data []byte) {
	f.mu.Lock()
	f.attachments[messageID+":"+attachmentID] = base64.RawURLEncoding.EncodeToString(data)
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailSetSendError(err error) {
	f.mu.Lock()
	f.sendFails = err
	f.mu.Unlock()
}

func (f *gmailFakeAPI) gmailSentSnapshot() []gmailTestSent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]gmailTestSent(nil), f.sent...)
}

type gmailTestRigOptions struct {
	Account      GmailAccountConfig
	Fake         *gmailFakeAPI
	Sleep        func(context.Context, time.Duration) error
	Jitter       func(time.Duration) time.Duration
	Log          func(string, ...any)
	PollInterval time.Duration
}

type gmailTestRig struct {
	store     *Store
	fake      *gmailFakeAPI
	connector *GmailConnector
	hostTools *HostTools
	directory string
}

func gmailNewTestRig(t *testing.T, options gmailTestRigOptions) *gmailTestRig {
	t.Helper()
	directory := t.TempDir()
	store, err := Open(filepath.Join(directory, "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	fake := options.Fake
	if fake == nil {
		fake = gmailNewFakeAPI()
	}
	fake.gmailStartServer(t)
	account := options.Account
	if account.Email == "" {
		account = GmailAccountConfig{Email: gmailTestAccount, TokenCommand: "true"}
	}
	connector, err := NewGmailConnector(GmailConnectorConfig{
		Store: store, Target: gmailTestTarget, Accounts: []GmailAccountConfig{account},
		AttachmentDir: filepath.Join(directory, "attachments"), PollInterval: options.PollInterval,
		ClientFactory: func(GmailAccountConfig) (GmailAPI, error) {
			return NewGmailClient(GmailClientConfig{
				Email: gmailTestAccount, Tokens: &gmailTestTokenSource{}, BaseURL: fake.server.URL,
			})
		},
		Now: func() int64 { return 1_700_000_000_000 }, Sleep: options.Sleep,
		Jitter: options.Jitter, Log: options.Log,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(connector); err != nil {
		t.Fatal(err)
	}
	hostTools, err := NewHostTools(HostToolsOptions{
		Store: store, Connectors: registry, Now: func() int64 { return 1_700_000_000_000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &gmailTestRig{store: store, fake: fake, connector: connector, hostTools: hostTools, directory: directory}
}

func gmailTestMessage(id, threadID, from, subject, body string, labels []string) GmailMessage {
	if threadID == "" {
		threadID = id
	}
	if from == "" {
		from = "Dana <dana@example.com>"
	}
	if subject == "" {
		subject = "a question"
	}
	if body == "" {
		body = "body text"
	}
	if labels == nil {
		labels = []string{"INBOX"}
	}
	return GmailMessage{
		ID: id, ThreadID: threadID, LabelIDs: labels, InternalDate: "1700000000000",
		Payload: &GmailPart{
			MIMEType: "text/plain",
			Headers: []GmailHeader{
				{Name: "From", Value: from}, {Name: "To", Value: gmailTestAccount},
				{Name: "Subject", Value: subject}, {Name: "Message-ID", Value: "<" + id + "@mail>"},
			},
			Body: &GmailPartBody{Data: base64.RawURLEncoding.EncodeToString([]byte(body))},
		},
	}
}

func gmailSetTestWatermark(t *testing.T, store *Store, historyID string) {
	t.Helper()
	if err := store.SetWatermark(gmailTestAccount, historyID, 1_700_000_000_000); err != nil {
		t.Fatal(err)
	}
}

func gmailTestEventCount(t *testing.T, store *Store) int64 {
	t.Helper()
	count, err := store.CountEvents()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func gmailFindTestEvent(t *testing.T, store *Store, key string) *Event {
	t.Helper()
	event, err := store.FindEvent(GmailName, key)
	if err != nil || event == nil {
		t.Fatalf("FindEvent() = %#v, %v", event, err)
	}
	return event
}

func gmailDecodeTestMIME(t *testing.T, raw string) string {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		t.Fatal(err)
	}
	return string(decoded)
}

func TestGmailTokenLastLineAndCache(t *testing.T) {
	directory := t.TempDir()
	script := filepath.Join(directory, "token.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho fetching >&2\necho noise\necho tok-abc123\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	provider := NewGmailTokenProvider(script, gmailTestAccount)
	token, err := provider.Get(context.Background())
	if err != nil || token != "tok-abc123" {
		t.Fatalf("Get() = %q, %v", token, err)
	}
	if err := os.Remove(script); err != nil {
		t.Fatal(err)
	}
	token, err = provider.Get(context.Background())
	if err != nil || token != "tok-abc123" {
		t.Fatalf("cached Get() = %q, %v", token, err)
	}
}

func TestGmailTokenFailureDiagnostics(t *testing.T) {
	provider := NewGmailTokenProvider(`echo "gcloud: not logged in" >&2; exit 3`, gmailTestAccount)
	_, err := provider.Get(context.Background())
	if err == nil || !strings.Contains(err.Error(), gmailTestAccount) || !strings.Contains(err.Error(), "not logged in") {
		t.Fatalf("Get() error = %v", err)
	}
}
func TestGmailTokenFailureDiagnosticsExcludeCredentialTail(t *testing.T) {
	const head = "safe diagnostic begins here"
	const credential = "client_secret=credential-sentinel-must-not-leak"
	stderr := head + strings.Repeat("x", 600) + credential
	provider := NewGmailTokenProviderWithConfig(GmailTokenProviderConfig{
		Command: "ignored",
		Label:   gmailTestAccount,
		Run: func(context.Context, string) (string, string, int, error) {
			return "", stderr, 3, fmt.Errorf("exit status 3")
		},
	})

	_, err := provider.Get(context.Background())
	if err == nil {
		t.Fatal("failing token command returned no error")
	}
	if !strings.Contains(err.Error(), head) {
		t.Fatalf("error omitted diagnostic head: %v", err)
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("error leaked credential-like stderr tail: %v", err)
	}
}

func TestGmailTokenEmptyOutputRejected(t *testing.T) {
	provider := NewGmailTokenProvider("true", gmailTestAccount)
	if _, err := provider.Get(context.Background()); err == nil || !strings.Contains(err.Error(), "printed no token") {
		t.Fatalf("Get() error = %v", err)
	}
}

func TestGmailTokenRefreshSingleflight(t *testing.T) {
	directory := t.TempDir()
	marks := filepath.Join(directory, "runs")
	provider := NewGmailTokenProvider(fmt.Sprintf("echo x >> %q; echo tok", marks), gmailTestAccount)
	const callers = 16
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			token, err := provider.Get(context.Background())
			if err == nil && token != "tok" {
				err = fmt.Errorf("token = %q", token)
			}
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	contents, err := os.ReadFile(marks)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Fields(string(contents)); len(lines) != 1 {
		t.Fatalf("command runs = %d, want 1", len(lines))
	}
}

func TestGmailInboxEventIdentityAndThread(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "t1", "", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	event := gmailFindTestEvent(t, rig.store, gmailTestAccount+":m1")
	if event.ConversationID != "t1" || event.User == nil || *event.User != "dana@example.com" || !strings.Contains(event.Content, "a question") {
		t.Fatalf("event = %#v", event)
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(event.MetaJSON), &meta); err != nil || meta["account"] != gmailTestAccount || meta["from_email"] != "dana@example.com" {
		t.Fatalf("meta = %#v, %v", meta, err)
	}
	delivery, err := rig.store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil || delivery.Target != gmailTestTarget {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
}

func TestGmailSameThreadMessagesShareConversation(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "t1", "", "", "", nil))
	rig.fake.gmailAddMessage(gmailTestMessage("m2", "t1", "", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1", "m2"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != 2 {
		t.Fatalf("event count = %d", gmailTestEventCount(t, rig.store))
	}
	for _, id := range []string{"m1", "m2"} {
		if event := gmailFindTestEvent(t, rig.store, gmailTestAccount+":"+id); event.ConversationID != "t1" {
			t.Fatalf("event %s conversation = %s", id, event.ConversationID)
		}
	}
}

func TestGmailDefaultExcludedLabelsDropMessages(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "", "", "", "", []string{"INBOX", "CATEGORY_PROMOTIONS"}))
	rig.fake.gmailAddMessage(gmailTestMessage("m2", "", "", "", "", []string{"INBOX", "SENT"}))
	rig.fake.gmailAddMessage(gmailTestMessage("m3", "", "", "", "", []string{"SPAM"}))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1", "m2", "m3"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != 0 {
		t.Fatal("excluded mail was stored")
	}
}

func TestGmailSelfAuthoredMailIsDropped(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "", "Gateway <"+gmailTestAccount+">", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != 0 {
		t.Fatal("self-authored mail was stored")
	}
}

func TestGmailVanishedMessageIsSkipped(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"gone"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != 0 {
		t.Fatal("vanished mail was stored")
	}
}

func TestGmailDuplicateIDIsOneRow(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "", "", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}}, gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != 1 {
		t.Fatalf("event count = %d", gmailTestEventCount(t, rig.store))
	}
	deliveries, err := rig.store.DeliveriesForTarget(gmailTestTarget)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, %v", deliveries, err)
	}
}

func TestGmailNoWatermarkBootstrapsWithoutDelivery(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	rig.fake.gmailSetHistoryID("5000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "", "", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	watermark, err := rig.store.GetWatermark(gmailTestAccount)
	if err != nil || watermark == nil || *watermark != "5000" || gmailTestEventCount(t, rig.store) != 0 {
		t.Fatalf("watermark=%v events=%d err=%v", watermark, gmailTestEventCount(t, rig.store), err)
	}
}

func TestGmailWatermarkAdvancesAfterIngest(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailSetHistoryID("1500")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "", "", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	watermark, err := rig.store.GetWatermark(gmailTestAccount)
	if err != nil || watermark == nil || *watermark != "1500" {
		t.Fatalf("watermark = %v, %v", watermark, err)
	}
}

func TestGmailExpiredWatermarkRebootstrapsLoudly(t *testing.T) {
	var logsMu sync.Mutex
	var logs []string
	rig := gmailNewTestRig(t, gmailTestRigOptions{Log: func(format string, args ...any) {
		logsMu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		logsMu.Unlock()
	}})
	gmailSetTestWatermark(t, rig.store, "ancient")
	rig.fake.gmailSetHistoryID("9000")
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{Err: &GmailHTTPError{Status: 404, Message: "historyId too old"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	watermark, err := rig.store.GetWatermark(gmailTestAccount)
	if err != nil || watermark == nil || *watermark != "9000" || gmailTestEventCount(t, rig.store) != 0 {
		t.Fatalf("watermark=%v events=%d err=%v", watermark, gmailTestEventCount(t, rig.store), err)
	}
	logsMu.Lock()
	joined := strings.Join(logs, "\n")
	logsMu.Unlock()
	if !strings.Contains(joined, "WARN") || !strings.Contains(joined, "NOT delivered") {
		t.Fatalf("logs = %q", joined)
	}
}

func TestGmailNon404PollErrorPropagates(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{Err: &GmailHTTPError{Status: 500, Message: "upstream"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err == nil {
		t.Fatal("500 history error was swallowed")
	}
	watermark, err := rig.store.GetWatermark(gmailTestAccount)
	if err != nil || watermark == nil || *watermark != "1000" {
		t.Fatalf("watermark = %v, %v", watermark, err)
	}
}

func gmailPrepareReplyEvent(t *testing.T, rig *gmailTestRig) (*Event, *Delivery) {
	t.Helper()
	gmailSetTestWatermark(t, rig.store, "1000")
	rig.fake.gmailAddMessage(gmailTestMessage("m1", "t1", "", "a question", "", nil))
	rig.fake.gmailSetThread(GmailThreadMetadata{ID: "t1", Messages: []GmailMessage{{
		ID: "m1", ThreadID: "t1", Payload: &GmailPart{Headers: []GmailHeader{
			{Name: "Message-ID", Value: "<m1@mail>"}, {Name: "References", Value: "<earlier@mail>"},
			{Name: "Subject", Value: "a question"},
		}},
	}}})
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	event := gmailFindTestEvent(t, rig.store, gmailTestAccount+":m1")
	delivery, err := rig.store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
	return event, delivery
}

func gmailChatReply(t *testing.T, rig *gmailTestRig, deliveryID, conversationID, message string) ToolResult {
	t.Helper()
	result, err := rig.hostTools.ChatReply(context.Background(), map[string]any{
		"agent": gmailTestTarget, "delivery_id": deliveryID,
		"conversation_id": conversationID, "message": message,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestGmailReplyMailboxRecipientAndThreading(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	_, delivery := gmailPrepareReplyEvent(t, rig)
	if result := gmailChatReply(t, rig, delivery.ID, "t1", "the answer"); result.IsError {
		t.Fatalf("reply = %#v", result)
	}
	sent := rig.fake.gmailSentSnapshot()
	if len(sent) != 1 || sent[0].ThreadID != "t1" {
		t.Fatalf("sent = %#v", sent)
	}
	mime := gmailDecodeTestMIME(t, sent[0].Raw)
	for _, expected := range []string{
		"From: " + gmailTestAccount, "To: dana@example.com", "Subject: Re: a question",
		"In-Reply-To: <m1@mail>", "References: <earlier@mail> <m1@mail>",
	} {
		if !strings.Contains(mime, expected) {
			t.Errorf("MIME lacks %q:\n%s", expected, mime)
		}
	}
	if subject := GmailReplySubject("Re: already threaded"); subject != "Re: already threaded" {
		t.Fatalf("reply subject stacked prefix: %q", subject)
	}
}

func TestGmailConfirmedSendHandlesEvent(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	event, delivery := gmailPrepareReplyEvent(t, rig)
	if result := gmailChatReply(t, rig, delivery.ID, "t1", "x"); result.IsError {
		t.Fatalf("reply = %#v", result)
	}
	event, err := rig.store.GetEvent(event.ID)
	if err != nil || event.HandledAt == nil {
		t.Fatalf("event = %#v, %v", event, err)
	}
}

func TestGmailFailedSendLeavesReplied(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	event, delivery := gmailPrepareReplyEvent(t, rig)
	rig.fake.gmailSetSendError(fmt.Errorf("HTTP 503"))
	if result := gmailChatReply(t, rig, delivery.ID, "t1", "x"); !result.IsError {
		t.Fatalf("reply = %#v", result)
	}
	event, err := rig.store.GetEvent(event.ID)
	if err != nil || event.HandledAt != nil {
		t.Fatalf("event = %#v, %v", event, err)
	}
	delivery, err = rig.store.GetDelivery(delivery.ID)
	if err != nil || delivery.Status != DeliveryReplied {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
}

func TestGmailMissingAccountMetaRefuses(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	meta, _ := json.Marshal(map[string]string{"from_email": "someone@example.com"})
	event, err := rig.store.InsertEvent(EventInsert{
		Connector: GmailName, EventKey: gmailTestAccount + ":broken", ConversationID: "t9",
		Content: "x", MetaJSON: string(meta),
	}, 1_700_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := rig.store.InsertDelivery(event.ID, gmailTestTarget, 1_700_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	err = rig.connector.PostReply(context.Background(), DeliveryContext{Delivery: *delivery, Event: *event, ConversationID: "t9"}, "hi")
	if err == nil || !strings.Contains(err.Error(), "meta.account") || len(rig.fake.gmailSentSnapshot()) != 0 {
		t.Fatalf("PostReply() error=%v sent=%#v", err, rig.fake.gmailSentSnapshot())
	}
}

func TestGmailSentMessageDoesNotLoopBack(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	event, delivery := gmailPrepareReplyEvent(t, rig)
	if err := rig.connector.PostReply(context.Background(), DeliveryContext{Delivery: *delivery, Event: *event, ConversationID: "t1"}, "answer"); err != nil {
		t.Fatal(err)
	}
	rig.fake.gmailAddMessage(gmailTestMessage("sent-1", "t1", "Someone <x@y.z>", "", "", nil))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"sent-1"}})
	before := gmailTestEventCount(t, rig.store)
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	if gmailTestEventCount(t, rig.store) != before {
		t.Fatal("sent message looped back")
	}
}

func TestGmailSendStartsNewThread(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	result, err := rig.connector.CallTool(context.Background(), "gmail_send", map[string]any{
		"account": gmailTestAccount, "to": "rachel@example.com",
		"subject": "a heads-up", "body": "details",
	})
	if err != nil || result.IsError {
		t.Fatalf("CallTool() = %#v, %v", result, err)
	}
	sent := rig.fake.gmailSentSnapshot()
	if len(sent) != 1 || sent[0].ThreadID != "" || !strings.Contains(gmailDecodeTestMIME(t, sent[0].Raw), "Subject: a heads-up") {
		t.Fatalf("sent = %#v", sent)
	}
}

func TestGmailReplyWithoutThreadRefuses(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	result, err := rig.connector.CallTool(context.Background(), "gmail_reply", map[string]any{
		"account": gmailTestAccount, "to": "rachel@example.com", "body": "details",
	})
	if err != nil || !result.IsError || len(rig.fake.gmailSentSnapshot()) != 0 {
		t.Fatalf("CallTool() = %#v, %v sent=%#v", result, err, rig.fake.gmailSentSnapshot())
	}
}

func TestGmailUnknownAccountNamesWatched(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	_, err := rig.connector.CallTool(context.Background(), "gmail_send", map[string]any{
		"account": "nobody@example.com", "to": "x@y.z", "subject": "s", "body": "b",
	})
	if err == nil || !strings.Contains(err.Error(), gmailTestAccount) {
		t.Fatalf("CallTool() error = %v", err)
	}
}

func TestGmailAttachmentPersistsAndRendersPath(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	gmailSetTestWatermark(t, rig.store, "1000")
	message := gmailTestMessage("m1", "", "", "", "", nil)
	message.Payload.Parts = []GmailPart{
		{MIMEType: "text/plain", Body: &GmailPartBody{Data: base64.RawURLEncoding.EncodeToString([]byte("see attached"))}},
		{Filename: "notes.txt", MIMEType: "text/plain", Body: &GmailPartBody{AttachmentID: "att1", Size: 5}},
	}
	rig.fake.gmailAddMessage(message)
	rig.fake.gmailSetAttachment("m1", "att1", []byte("hello"))
	rig.fake.gmailQueueHistory(gmailTestHistoryResult{IDs: []string{"m1"}})
	if err := rig.connector.PollOnce(context.Background(), gmailTestAccount); err != nil {
		t.Fatal(err)
	}
	event := gmailFindTestEvent(t, rig.store, gmailTestAccount+":m1")
	pathStart := strings.Index(event.Content, filepath.Join(rig.directory, "attachments"))
	if pathStart < 0 {
		t.Fatalf("content has no attachment path: %s", event.Content)
	}
	pathEnd := strings.IndexAny(event.Content[pathStart:], " \n")
	path := event.Content[pathStart:]
	if pathEnd >= 0 {
		path = path[:pathEnd]
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "hello" {
		t.Fatalf("attachment %q = %q, %v", path, contents, err)
	}
}

func TestGmailPollJitterAndErrorBackoff(t *testing.T) {
	if low, high := gmailJitter(20*time.Second, 0), gmailJitter(20*time.Second, 1); low != 16*time.Second || high != 24*time.Second {
		t.Fatalf("default jitter bounds = %s..%s", low, high)
	}
	for _, test := range []struct {
		name string
		err  error
		want time.Duration
	}{
		{name: "success", want: 20 * time.Second},
		{name: "error", err: &GmailHTTPError{Status: 500, Message: "upstream"}, want: 40 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			delays := make(chan time.Duration, 1)
			fake := gmailNewFakeAPI()
			fake.gmailQueueHistory(gmailTestHistoryResult{Err: test.err})
			rig := gmailNewTestRig(t, gmailTestRigOptions{
				Fake: fake,
				Jitter: func(delay time.Duration) time.Duration {
					return delay * 4 / 5
				},
				Sleep: func(ctx context.Context, delay time.Duration) error {
					delays <- delay
					<-ctx.Done()
					return ctx.Err()
				},
			})
			gmailSetTestWatermark(t, rig.store, "1000")
			if err := rig.connector.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case delay := <-delays:
				if delay != test.want*4/5 {
					t.Fatalf("sleep delay = %s, want %s", delay, test.want*4/5)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("poll loop did not sleep")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := rig.connector.Stop(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGmailTokenCacheExpiryRefreshes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var runs atomic.Int64
	provider := NewGmailTokenProviderWithConfig(GmailTokenProviderConfig{
		Command: "ignored", Label: gmailTestAccount, MaxAge: GmailTokenMaxAge,
		Now: func() time.Time { return now },
		Run: func(context.Context, string) (string, string, int, error) {
			return fmt.Sprintf("token-%d\n", runs.Add(1)), "", 0, nil
		},
	})
	first, err := provider.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(GmailTokenMaxAge - time.Second)
	cached, err := provider.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	refreshed, err := provider.Get(context.Background())
	if err != nil || first != cached || refreshed == cached || runs.Load() != 2 {
		t.Fatalf("first=%q cached=%q refreshed=%q runs=%d err=%v", first, cached, refreshed, runs.Load(), err)
	}
}

type gmailTestTokenSource struct {
	mu          sync.Mutex
	invalidated int
}

func (s *gmailTestTokenSource) Get(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invalidated == 0 {
		return "old", nil
	}
	return "new", nil
}

func (s *gmailTestTokenSource) Invalidate() {
	s.mu.Lock()
	s.invalidated++
	s.mu.Unlock()
}

func TestGmailClient401RetriesExactlyOnce(t *testing.T) {
	for _, test := range []struct {
		name         string
		alwaysReject bool
		wantErr      bool
	}{
		{name: "retry succeeds"},
		{name: "second 401 stops", alwaysReject: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				attempt := requests.Add(1)
				if attempt == 1 || test.alwaysReject {
					http.Error(w, "stale", http.StatusUnauthorized)
					return
				}
				_ = json.NewEncoder(w).Encode(GmailProfile{EmailAddress: gmailTestAccount, HistoryID: "1"})
			}))
			defer server.Close()
			tokens := &gmailTestTokenSource{}
			client, err := NewGmailClient(GmailClientConfig{Email: gmailTestAccount, Tokens: tokens, BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.GetProfile(context.Background())
			if (err != nil) != test.wantErr || requests.Load() != 2 {
				t.Fatalf("error=%v requests=%d", err, requests.Load())
			}
			tokens.mu.Lock()
			invalidated := tokens.invalidated
			tokens.mu.Unlock()
			if invalidated != 1 {
				t.Fatalf("invalidations = %d", invalidated)
			}
		})
	}
}

func TestGmailCustomLabelVerdictMatrix(t *testing.T) {
	message := gmailTestMessage("m1", "", "Sender <sender@example.com>", "", "", []string{"INBOX", "STARRED"})
	for _, test := range []struct {
		name    string
		from    string
		options GmailFilterOptions
		want    bool
	}{
		{name: "custom required present", from: "sender@example.com", options: GmailFilterOptions{LabelsRequire: []string{"STARRED"}, LabelsExclude: []string{}}, want: true},
		{name: "custom required missing", from: "sender@example.com", options: GmailFilterOptions{LabelsRequire: []string{"IMPORTANT"}, LabelsExclude: []string{}}, want: false},
		{name: "custom excluded", from: "sender@example.com", options: GmailFilterOptions{LabelsRequire: []string{}, LabelsExclude: []string{"STARRED"}}, want: false},
		{name: "self authored always drops", from: gmailTestAccount, options: GmailFilterOptions{LabelsRequire: []string{}, LabelsExclude: []string{}}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := GmailDeliveryDecision(message, test.from, gmailTestAccount, test.options).Deliver; got != test.want {
				t.Fatalf("Deliver = %t, want %t", got, test.want)
			}
		})
	}
}

func TestGmailMalformedAccountsFatalAndAbsentInactive(t *testing.T) {
	accounts, active, err := LoadGmailAccounts(GmailOptions{})
	if err != nil || active || accounts != nil {
		t.Fatalf("absent options = %#v, %t, %v", accounts, active, err)
	}
	for name, options := range map[string]GmailOptions{
		"inline": {Enabled: true, AccountsJSON: "not json"},
		"file":   {Enabled: true, AccountsFile: filepath.Join(t.TempDir(), "missing.json")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, active, err := LoadGmailAccounts(options); !active || err == nil {
				t.Fatalf("active=%t error=%v", active, err)
			}
		})
	}
}

func TestGmailPostReplyReadsEventMetadata(t *testing.T) {
	rig := gmailNewTestRig(t, gmailTestRigOptions{})
	threadID := "opaque-thread-that-contains-no-address"
	rig.fake.gmailSetThread(GmailThreadMetadata{ID: threadID, Messages: []GmailMessage{{
		ID: "last", ThreadID: threadID, Payload: &GmailPart{Headers: []GmailHeader{
			{Name: "Message-ID", Value: "<last@mail>"}, {Name: "Subject", Value: "topic"},
		}},
	}}})
	meta, _ := json.Marshal(map[string]string{"account": gmailTestAccount, "from_email": "meta-recipient@example.com"})
	event := Event{ID: 99, ConversationID: threadID, MetaJSON: string(meta)}
	if err := rig.connector.PostReply(context.Background(), DeliveryContext{Event: event, ConversationID: threadID}, "answer"); err != nil {
		t.Fatal(err)
	}
	sent := rig.fake.gmailSentSnapshot()
	if len(sent) != 1 || sent[0].ThreadID != threadID || !strings.Contains(gmailDecodeTestMIME(t, sent[0].Raw), "To: meta-recipient@example.com") {
		t.Fatalf("sent = %#v", sent)
	}
}
