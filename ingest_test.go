package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const (
	ingestTestSecret = "test-secret-at-least-16-bytes"
	ingestTestNow    = int64(1_700_000_000_000)
	ingestTestTarget = "agent-one"
)

type ingestTestClient func(*http.Request) (*http.Response, error)

func (client ingestTestClient) Do(request *http.Request) (*http.Response, error) {
	return client(request)
}

func newIngestTestHost(t *testing.T, sources ...IngestSource) (*Store, *IngestHost, []*IngestConnector) {
	t.Helper()
	store := openTestStore(t)
	host, err := NewIngestHost(IngestHostConfig{
		Port:   8791,
		Store:  store,
		Target: ingestTestTarget,
		Now:    func() int64 { return ingestTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	connectors := make([]*IngestConnector, 0, len(sources))
	for _, source := range sources {
		connector, err := host.Add(source)
		if err != nil {
			t.Fatal(err)
		}
		connectors = append(connectors, connector)
	}
	return store, host, connectors
}

func ingestTestSource(name string) IngestSource {
	return IngestSource{Source: name, Secret: ingestTestSecret}
}

func ingestTestBody(fields map[string]any) []byte {
	body, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return body
}

func ingestValidBody(key string) []byte {
	return ingestTestBody(map[string]any{
		"schema":          IngestSchema,
		"event_key":       key,
		"conversation_id": "conversation-1",
		"content":         "incoming content",
	})
}

func signedIngestRequest(source string, body []byte, timestamp int64, secret string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, IngestPathPrefix+source, bytes.NewReader(body))
	request.Header.Set(IngestTimestampHeader, strconv.FormatInt(timestamp, 10))
	request.Header.Set(IngestSignatureHeader, SignIngest(secret, timestamp, body))
	return request
}

func serveIngestRequest(host *IngestHost, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	host.ServeHTTP(recorder, request)
	return recorder
}

func ingestStoredCount(t *testing.T, store *Store) int64 {
	t.Helper()
	count, err := store.CountEvents()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func TestIngestSignatureRoundTripAndAccepts(t *testing.T) {
	store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
	body := ingestValidBody("signature-ok")
	timestamp := ingestTestNow / 1000
	signature := SignIngest(ingestTestSecret, timestamp, body)
	if !VerifyIngestSignature(ingestTestSecret, signature, timestamp, body) {
		t.Fatal("VerifyIngestSignature rejected SignIngest output")
	}

	response := serveIngestRequest(host, signedIngestRequest("alerts", body, timestamp, ingestTestSecret))
	if response.Code != http.StatusAccepted || ingestStoredCount(t, store) != 1 {
		t.Fatalf("ingest response = %d, events = %d", response.Code, ingestStoredCount(t, store))
	}
}

func TestIngestRejectsUnauthenticatedBodies(t *testing.T) {
	timestamp := ingestTestNow / 1000
	body := ingestValidBody("auth-rejected")
	tests := []struct {
		name    string
		request func() *http.Request
	}{
		{
			name: "wrong secret",
			request: func() *http.Request {
				return signedIngestRequest("alerts", body, timestamp, "another-secret-at-least-16")
			},
		},
		{
			name: "tampered body",
			request: func() *http.Request {
				tampered := append([]byte(nil), body...)
				tampered[len(tampered)-2] = 'X'
				request := signedIngestRequest("alerts", tampered, timestamp, ingestTestSecret)
				request.Header.Set(IngestSignatureHeader, SignIngest(ingestTestSecret, timestamp, body))
				return request
			},
		},
		{
			name: "missing signature",
			request: func() *http.Request {
				request := signedIngestRequest("alerts", body, timestamp, ingestTestSecret)
				request.Header.Del(IngestSignatureHeader)
				return request
			},
		},
		{
			name: "signature missing version prefix",
			request: func() *http.Request {
				request := signedIngestRequest("alerts", body, timestamp, ingestTestSecret)
				request.Header.Set(IngestSignatureHeader, strings.TrimPrefix(request.Header.Get(IngestSignatureHeader), IngestSignaturePrefix))
				return request
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
			response := serveIngestRequest(host, test.request())
			if response.Code != http.StatusUnauthorized || ingestStoredCount(t, store) != 0 {
				t.Fatalf("response = %d, events = %d", response.Code, ingestStoredCount(t, store))
			}
		})
	}
}

func TestIngestRejectsInvalidTimestamps(t *testing.T) {
	body := ingestValidBody("timestamp")
	now := ingestTestNow / 1000
	tests := []struct {
		name      string
		timestamp string
		valid     bool
	}{
		{name: "missing", timestamp: ""},
		{name: "non numeric", timestamp: "not-a-time"},
		{name: "301 seconds past", timestamp: strconv.FormatInt(now-301, 10)},
		{name: "301 seconds future", timestamp: strconv.FormatInt(now+301, 10)},
		{name: "299 seconds past", timestamp: strconv.FormatInt(now-299, 10), valid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
			request := signedIngestRequest("alerts", body, now, ingestTestSecret)
			if test.timestamp == "" {
				request.Header.Del(IngestTimestampHeader)
			} else {
				request.Header.Set(IngestTimestampHeader, test.timestamp)
				if timestamp, err := strconv.ParseInt(test.timestamp, 10, 64); err == nil {
					request.Header.Set(IngestSignatureHeader, SignIngest(ingestTestSecret, timestamp, body))
				}
			}
			response := serveIngestRequest(host, request)
			wantStatus := http.StatusUnauthorized
			wantEvents := int64(0)
			if test.valid {
				wantStatus = http.StatusAccepted
				wantEvents = 1
			}
			if response.Code != wantStatus || ingestStoredCount(t, store) != wantEvents {
				t.Fatalf("response = %d, events = %d; want %d, %d", response.Code, ingestStoredCount(t, store), wantStatus, wantEvents)
			}
		})
	}
}

func TestIngestRejectsInvalidPayloads(t *testing.T) {
	meta := make(map[string]any, ingestMaxMetaEntries+1)
	for index := range ingestMaxMetaEntries + 1 {
		meta["key"+strconv.Itoa(index)] = "value"
	}
	longContent := strings.Repeat("x", ingestMaxContentBytes+1)
	tests := []struct {
		name string
		body []byte
	}{
		{name: "malformed JSON", body: []byte(`{"schema":`)},
		{name: "missing schema", body: ingestTestBody(map[string]any{"event_key": "bad", "conversation_id": "c", "content": "x"})},
		{name: "other schema", body: ingestTestBody(map[string]any{"schema": "courier.ingest/2", "event_key": "bad", "conversation_id": "c", "content": "x"})},
		{name: "missing event key", body: ingestTestBody(map[string]any{"schema": IngestSchema, "conversation_id": "c", "content": "x"})},
		{name: "missing conversation ID", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "content": "x"})},
		{name: "missing content", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c"})},
		{name: "oversized content", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c", "content": longContent})},
		{name: "oversized event key", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": strings.Repeat("x", ingestMaxIDChars+1), "conversation_id": "c", "content": "x"})},
		{name: "oversized trigger", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c", "content": "x", "trigger": strings.Repeat("x", MaxTriggerChars+1)})},
		{name: "too many meta entries", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c", "content": "x", "meta": meta})},
		{name: "non string meta value", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c", "content": "x", "meta": map[string]any{"number": 1}})},
		{name: "reserved meta trigger", body: ingestTestBody(map[string]any{"schema": IngestSchema, "event_key": "bad", "conversation_id": "c", "content": "x", "meta": map[string]any{"trigger": "wrong"}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
			response := serveIngestRequest(host, signedIngestRequest("alerts", test.body, ingestTestNow/1000, ingestTestSecret))
			if response.Code != http.StatusBadRequest || ingestStoredCount(t, store) != 0 {
				t.Fatalf("response = %d, events = %d", response.Code, ingestStoredCount(t, store))
			}
		})
	}
}

func TestIngestIgnoresUnknownPayloadFields(t *testing.T) {
	store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
	body := ingestTestBody(map[string]any{
		"schema":          IngestSchema,
		"event_key":       "future-field",
		"conversation_id": "conversation-1",
		"content":         "still valid",
		"future_field":    map[string]any{"nested": true},
	})
	response := serveIngestRequest(host, signedIngestRequest("alerts", body, ingestTestNow/1000, ingestTestSecret))
	if response.Code != http.StatusAccepted || ingestStoredCount(t, store) != 1 {
		t.Fatalf("response = %d, events = %d", response.Code, ingestStoredCount(t, store))
	}
}

func TestIngestRoutesAndLimits(t *testing.T) {
	store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
	unknown := serveIngestRequest(host, httptest.NewRequest(http.MethodPost, "/ingest/unknown", nil))
	wrongMethod := serveIngestRequest(host, httptest.NewRequest(http.MethodGet, "/ingest/alerts", nil))
	wrongPath := serveIngestRequest(host, httptest.NewRequest(http.MethodPost, "/other", nil))
	large := serveIngestRequest(host, httptest.NewRequest(http.MethodPost, "/ingest/alerts", bytes.NewReader(bytes.Repeat([]byte("x"), ingestMaxBodyBytes+1))))
	if unknown.Code != http.StatusNotFound || wrongMethod.Code != http.StatusNotFound || wrongPath.Code != http.StatusNotFound || large.Code != http.StatusRequestEntityTooLarge || ingestStoredCount(t, store) != 0 {
		t.Fatalf("statuses unknown=%d method=%d path=%d large=%d events=%d", unknown.Code, wrongMethod.Code, wrongPath.Code, large.Code, ingestStoredCount(t, store))
	}
}

func TestIngestStoresExpectedLedgerRow(t *testing.T) {
	store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"))
	body := []byte(`{"schema":"courier.ingest/1","event_key":"ledger-row","conversation_id":"conversation:42","content":"verbatim\ncontent","trigger":"mention","meta":{"url":"https://example.test/item"}}`)
	response := serveIngestRequest(host, signedIngestRequest("alerts", body, ingestTestNow/1000, ingestTestSecret))
	if response.Code != http.StatusAccepted {
		t.Fatalf("response = %d: %s", response.Code, response.Body.String())
	}
	event, err := store.FindEvent("alerts", "ledger-row")
	if err != nil || event == nil {
		t.Fatalf("FindEvent = %#v, %v", event, err)
	}
	if event.Connector != "alerts" || event.EventKey != "ledger-row" || event.ConversationID != "conversation:42" || event.User != nil || event.Content != "verbatim\ncontent" || event.RawJSON != string(body) {
		t.Fatalf("stored event = %#v", event)
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(event.MetaJSON), &meta); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta, map[string]string{"url": "https://example.test/item", "trigger": "mention"}) {
		t.Fatalf("meta = %#v", meta)
	}
	if trigger, ok := TriggerOf(event.MetaJSON); !ok || trigger != "mention" {
		t.Fatalf("TriggerOf(%q) = %q, %v", event.MetaJSON, trigger, ok)
	}
	delivery, err := store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil || delivery.Target != ingestTestTarget || delivery.Status != DeliveryPending {
		t.Fatalf("delivery = %#v, %v", delivery, err)
	}
}

func TestIngestDeduplicatesPerSource(t *testing.T) {
	store, host, _ := newIngestTestHost(t, ingestTestSource("alerts"), ingestTestSource("builds"))
	body := ingestValidBody("shared-key")
	first := serveIngestRequest(host, signedIngestRequest("alerts", body, ingestTestNow/1000, ingestTestSecret))
	duplicate := serveIngestRequest(host, signedIngestRequest("alerts", body, ingestTestNow/1000, ingestTestSecret))
	otherSource := serveIngestRequest(host, signedIngestRequest("builds", body, ingestTestNow/1000, ingestTestSecret))
	if first.Code != http.StatusAccepted || duplicate.Code != http.StatusOK || otherSource.Code != http.StatusAccepted || ingestStoredCount(t, store) != 2 {
		t.Fatalf("statuses first=%d duplicate=%d other=%d events=%d", first.Code, duplicate.Code, otherSource.Code, ingestStoredCount(t, store))
	}
	deliveries, err := store.DeliveriesForTarget(ingestTestTarget)
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v, %v", deliveries, err)
	}
	var response map[string]any
	if err := json.Unmarshal(duplicate.Body.Bytes(), &response); err != nil || response["status"] != "duplicate" {
		t.Fatalf("duplicate response = %q, %v", duplicate.Body.String(), err)
	}
	for _, source := range []string{"alerts", "builds"} {
		event, err := store.FindEvent(source, "shared-key")
		if err != nil || event == nil {
			t.Fatalf("FindEvent(%q) = %#v, %v", source, event, err)
		}
		delivery, err := store.OpenDeliveryForEvent(event.ID)
		if err != nil || delivery == nil {
			t.Fatalf("delivery for %q = %#v, %v", source, delivery, err)
		}
	}
}

func TestIngestPostReplySignsPayload(t *testing.T) {
	var received IngestReplyPayload
	var body []byte
	var signature string
	var timestamp int64
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		signature = request.Header.Get(IngestSignatureHeader)
		timestamp, err = strconv.ParseInt(request.Header.Get(IngestTimestampHeader), 10, 64)
		if err != nil {
			t.Error(err)
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Error(err)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	_, _, connectors := newIngestTestHost(t, IngestSource{Source: "alerts", Secret: ingestTestSecret, ReplyURL: receiver.URL})
	user := "sender"
	err := connectors[0].PostReply(context.Background(), DeliveryContext{
		Delivery:       Delivery{ID: "delivery-1", Target: ingestTestTarget},
		Event:          Event{EventKey: "event-1", User: &user},
		ConversationID: "conversation-1",
	}, " answer ")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyIngestSignature(ingestTestSecret, signature, timestamp, body) {
		t.Fatal("reply callback signature did not verify")
	}
	want := IngestReplyPayload{Schema: IngestSchema, Kind: ingestReplyKind, Source: "alerts", ConversationID: "conversation-1", EventKey: "event-1", DeliveryID: "delivery-1", User: "sender", Agent: ingestTestTarget, Message: "answer"}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("reply payload = %#v, want %#v", received, want)
	}
}

func TestIngestPostReplyRequiresConfirmedResponse(t *testing.T) {
	for _, test := range []struct {
		name   string
		client ingestHTTPClient
	}{
		{
			name: "non 2xx",
			client: ingestTestClient(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("not durable"))}, nil
			}),
		},
		{
			name: "transport error",
			client: ingestTestClient(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t)
			host, err := NewIngestHost(IngestHostConfig{Port: 8791, Store: store, Target: ingestTestTarget, Client: test.client, Now: func() int64 { return ingestTestNow }})
			if err != nil {
				t.Fatal(err)
			}
			connector, err := host.Add(IngestSource{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "http://receiver.test/reply"})
			if err != nil {
				t.Fatal(err)
			}
			if err := connector.PostReply(context.Background(), DeliveryContext{Delivery: Delivery{ID: "delivery-1", Target: ingestTestTarget}, Event: Event{EventKey: "event-1"}, ConversationID: "conversation-1"}, "answer"); err == nil {
				t.Fatal("PostReply succeeded without a 2xx response")
			}
		})
	}
}

func TestIngestPostReplyShadowRefusesWithoutHTTPCall(t *testing.T) {
	calls := 0
	store := openTestStore(t)
	host, err := NewIngestHost(IngestHostConfig{
		Port:   8791,
		Store:  store,
		Target: ingestTestTarget,
		Shadow: NewShadowMode(true),
		Client: ingestTestClient(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("should not post")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	connector, err := host.Add(IngestSource{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "http://receiver.test/reply"})
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.PostReply(context.Background(), DeliveryContext{Delivery: Delivery{ID: "delivery-1"}, Event: Event{EventKey: "event-1"}}, "answer"); err == nil || calls != 0 {
		t.Fatalf("PostReply error = %v, calls = %d", err, calls)
	}
}

func TestIngestOneWayRepliesAreRefusedBeforePersistence(t *testing.T) {
	store, _, connectors := newIngestTestHost(t, ingestTestSource("oneway"), IngestSource{Source: "twoway", Secret: ingestTestSecret, ReplyURL: "http://receiver.test/reply"})
	oneway, twoway := connectors[0], connectors[1]
	if refusal := oneway.RefuseReply(DeliveryContext{}); refusal == "" || !strings.Contains(refusal, "oneway") {
		t.Fatalf("one-way refusal = %q", refusal)
	}
	if refusal := twoway.RefuseReply(DeliveryContext{}); refusal != "" {
		t.Fatalf("two-way refusal = %q", refusal)
	}
	registry := NewRegistry()
	if err := registry.Register(oneway); err != nil {
		t.Fatal(err)
	}
	tools, err := NewHostTools(HostToolsOptions{Store: store, Connectors: registry, Now: func() int64 { return ingestTestNow }})
	if err != nil {
		t.Fatal(err)
	}
	event, err := store.InsertEvent(EventInsert{Connector: oneway.Name(), EventKey: "one-way-reply", ConversationID: "conversation-1", Content: "message"}, ingestTestNow)
	if err != nil || event == nil {
		t.Fatalf("InsertEvent = %#v, %v", event, err)
	}
	delivery, err := store.InsertDelivery(event.ID, ingestTestTarget, ingestTestNow)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tools.ChatReply(context.Background(), map[string]any{"agent": ingestTestTarget, "delivery_id": delivery.ID, "conversation_id": "conversation-1", "message": "answer"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := store.GetReplyByDelivery(delivery.ID, ingestTestTarget)
	if err != nil {
		t.Fatal(err)
	}
	storedDelivery, err := store.GetDelivery(delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError || result.Status != http.StatusBadRequest || reply != nil || storedDelivery.Status != DeliveryPending {
		t.Fatalf("result=%#v reply=%#v delivery=%#v", result, reply, storedDelivery)
	}
}

func TestParseIngestSources(t *testing.T) {
	valid := `[{"source":" alerts ","secret":" ` + ingestTestSecret + ` ","reply_url":" https://receiver.test/reply ","instructions":" investigate then reply "}]`
	sources, err := ParseIngestSources(valid)
	if err != nil {
		t.Fatal(err)
	}
	want := []IngestSource{{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "https://receiver.test/reply", Instructions: "investigate then reply"}}
	if !reflect.DeepEqual(sources, want) {
		t.Fatalf("sources = %#v, want %#v", sources, want)
	}
	longName := "a" + strings.Repeat("x", 32)
	tooLongInstructions := strings.Repeat("x", ingestMaxInstructions+1)
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "empty array", raw: "[]"},
		{name: "non JSON", raw: "not json"},
		{name: "uppercase name", raw: ingestSourceJSON(IngestSource{Source: "Alerts", Secret: ingestTestSecret})},
		{name: "leading digit", raw: ingestSourceJSON(IngestSource{Source: "1alerts", Secret: ingestTestSecret})},
		{name: "too long name", raw: ingestSourceJSON(IngestSource{Source: longName, Secret: ingestTestSecret})},
		{name: "empty name", raw: ingestSourceJSON(IngestSource{Secret: ingestTestSecret})},
		{name: "reserved name", raw: ingestSourceJSON(IngestSource{Source: "gmail", Secret: ingestTestSecret})},
		{name: "duplicate name", raw: `[{"source":"alerts","secret":"` + ingestTestSecret + `"},{"source":"alerts","secret":"` + ingestTestSecret + `"}]`},
		{name: "short secret", raw: ingestSourceJSON(IngestSource{Source: "alerts", Secret: "short"})},
		{name: "non HTTP URL", raw: ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "ftp://receiver.test/reply"})},
		{name: "URL without host", raw: ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "https:/reply"})},
		{name: "long instructions", raw: ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret, Instructions: tooLongInstructions})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if sources, err := ParseIngestSources(test.raw); err == nil || sources != nil {
				t.Fatalf("ParseIngestSources(%q) = %#v, %v", test.raw, sources, err)
			}
		})
	}
}

func ingestSourceJSON(sources ...IngestSource) string {
	raw, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func TestLoadIngestSources(t *testing.T) {
	if sources, active, err := LoadIngestSources(IngestOptions{}); err != nil || active || sources != nil {
		t.Fatalf("disabled LoadIngestSources = %#v, %v, %v", sources, active, err)
	}
	if _, active, err := LoadIngestSources(IngestOptions{Enabled: true}); err == nil || !active {
		t.Fatalf("enabled without sources active=%v err=%v", active, err)
	}
	if _, _, err := LoadIngestSources(IngestOptions{Enabled: true, SourcesFile: "/does/not/exist"}); err == nil || !strings.Contains(err.Error(), "COURIER_INGEST_SOURCES_FILE") {
		t.Fatalf("missing file error = %v", err)
	}
	fromJSON := ingestSourceJSON(IngestSource{Source: "json", Secret: ingestTestSecret})
	sources, active, err := LoadIngestSources(IngestOptions{Enabled: true, SourcesJSON: fromJSON, SourcesFile: "/does/not/exist"})
	if err != nil || !active || len(sources) != 1 || sources[0].Source != "json" {
		t.Fatalf("JSON precedence sources=%#v active=%v err=%v", sources, active, err)
	}
	path := t.TempDir() + "/sources.json"
	fromFile := ingestSourceJSON(IngestSource{Source: "file", Secret: ingestTestSecret})
	if err := os.WriteFile(path, []byte(fromFile), 0o600); err != nil {
		t.Fatal(err)
	}
	sources, active, err = LoadIngestSources(IngestOptions{Enabled: true, SourcesFile: path})
	if err != nil || !active || len(sources) != 1 || sources[0].Source != "file" {
		t.Fatalf("file sources=%#v active=%v err=%v", sources, active, err)
	}
}

func TestServeOptionsIngestConfiguration(t *testing.T) {
	base := map[string]string{"COURIER_ORG": "org", "COURIER_TARGET": "agent"}
	withPort := map[string]string{"COURIER_ORG": "org", "COURIER_TARGET": "agent", "COURIER_INGEST_LISTEN_PORT": "8791"}
	if _, err := serveOptionsFromEnv(mapLookup(withPort), nil); err == nil || !strings.Contains(err.Error(), "COURIER_INGEST_SOURCES_JSON or COURIER_INGEST_SOURCES_FILE") {
		t.Fatalf("port-only error = %v", err)
	}
	withJSON := map[string]string{"COURIER_ORG": "org", "COURIER_TARGET": "agent", "COURIER_INGEST_LISTEN_PORT": "8791", "COURIER_INGEST_SOURCES_JSON": ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret})}
	opts, err := serveOptionsFromEnv(mapLookup(withJSON), nil)
	if err != nil || !opts.Ingest.Enabled || opts.Ingest.ListenPort != 8791 {
		t.Fatalf("configured ingest options=%#v err=%v", opts.Ingest, err)
	}
	base["COURIER_INGEST_SOURCES_JSON"] = ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret})
	opts, err = serveOptionsFromEnv(mapLookup(base), nil)
	if err != nil || opts.Ingest.Enabled {
		t.Fatalf("sources without port options=%#v err=%v", opts.Ingest, err)
	}
}

func TestServeIngestCandidatesAndManifest(t *testing.T) {
	store := openTestStore(t)
	sources := []IngestSource{
		{Source: "alerts", Secret: ingestTestSecret, ReplyURL: "http://receiver.test/reply", Instructions: "Respond to alerts."},
		{Source: "builds", Secret: ingestTestSecret, Instructions: "Investigate build failures."},
	}
	candidates, err := serveIngestCandidates(store, ServeOptions{Target: ingestTestTarget, Ingest: IngestOptions{Enabled: true, ListenPort: 8791, SourcesJSON: ingestSourceJSON(sources...)}}, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != len(sources) {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	connectors := make([]Connector, 0, len(candidates))
	for index, candidate := range candidates {
		if candidate.name != sources[index].Source {
			t.Fatalf("candidate %d name = %q", index, candidate.name)
		}
		connector, err := candidate.build()
		if err != nil {
			t.Fatal(err)
		}
		if connector.Name() != sources[index].Source || len(connector.ManifestTools()) != 0 {
			t.Fatalf("candidate connector = %s with tools %#v", connector.Name(), connector.ManifestTools())
		}
		connectors = append(connectors, connector)
	}
	manifest, err := BuildManifest(BuildManifestOptions{Name: "test", Version: "1", Connectors: connectors})
	if err != nil {
		t.Fatal(err)
	}
	for _, instruction := range []string{"Respond to alerts.", "Investigate build failures.", "source is one-way"} {
		if !strings.Contains(manifest.Instructions, instruction) {
			t.Fatalf("manifest instructions omit %q: %q", instruction, manifest.Instructions)
		}
	}
}

func TestIngestListenerReferenceCountsSources(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t)
	host, err := NewIngestHost(IngestHostConfig{Port: port, Store: store, Target: ingestTestTarget})
	if err != nil {
		t.Fatal(err)
	}
	first, err := host.Add(ingestTestSource("alerts"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := host.Add(ingestTestSource("builds"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := host.Address()
	if address == "" {
		t.Fatal("first Start did not bind the listener")
	}
	if err := second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if host.Address() != address {
		t.Fatalf("second Start address = %q, want %q", host.Address(), address)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if host.Address() == "" {
		t.Fatal("first Stop released a listener still owned by the second source")
	}
	if err := second.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if host.Address() != "" {
		t.Fatalf("second Stop left listener bound at %q", host.Address())
	}
}

func TestParsePushArgs(t *testing.T) {
	parsed, err := parsePushArgs([]string{"--source", "alerts", "--conversation=conversation-1", "--event-key", "event-1", "--content=body", "--user", "sender", "--trigger=mention", "--url", "http://localhost", "--meta", "url=https://example.test", "--meta=severity=high"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.source != "alerts" || parsed.conversation != "conversation-1" || parsed.eventKey != "event-1" || parsed.content != "body" || !parsed.contentSet || parsed.user != "sender" || parsed.trigger != "mention" || parsed.url != "http://localhost" || !reflect.DeepEqual(parsed.meta, map[string]string{"url": "https://example.test", "severity": "high"}) {
		t.Fatalf("parsed args = %#v", parsed)
	}
	withoutContent, err := parsePushArgs([]string{"--source=alerts"})
	if err != nil || withoutContent.contentSet {
		t.Fatalf("args without content = %#v, %v", withoutContent, err)
	}
	for _, args := range [][]string{{"--source", "alerts", "--meta", "missing"}, {"--source", "alerts", "--unknown", "value"}, {"--content", "body"}} {
		if _, err := parsePushArgs(args); err == nil {
			t.Fatalf("parsePushArgs(%q) succeeded", args)
		}
	}
}

func TestPushResolveSecretAndURL(t *testing.T) {
	declared := ingestSourceJSON(IngestSource{Source: "alerts", Secret: ingestTestSecret})
	secret, err := pushResolveSecret(mapLookup(map[string]string{"COURIER_INGEST_SOURCES_JSON": declared, "COURIER_INGEST_SECRET": "fallback-secret-at-least-16"}), "alerts")
	if err != nil || secret != ingestTestSecret {
		t.Fatalf("declared secret resolution error = %v", err)
	}
	fallback := "fallback-secret-at-least-16"
	secret, err = pushResolveSecret(mapLookup(map[string]string{"COURIER_INGEST_SECRET": fallback}), "alerts")
	if err != nil || secret != fallback {
		t.Fatalf("fallback secret resolution error = %v", err)
	}
	if _, err := pushResolveSecret(mapLookup(map[string]string{}), "alerts"); err == nil {
		t.Fatal("secret resolution succeeded without a declaration or fallback")
	}
	url, err := pushResolveURL(mapLookup(map[string]string{"COURIER_INGEST_URL": "https://override.test", "COURIER_INGEST_LISTEN_PORT": "8791"}))
	if err != nil || url != "https://override.test" {
		t.Fatalf("URL override = %q, %v", url, err)
	}
	url, err = pushResolveURL(mapLookup(map[string]string{"COURIER_INGEST_LISTEN_PORT": "8791"}))
	if err != nil || url != "http://127.0.0.1:8791" {
		t.Fatalf("URL from port = %q, %v", url, err)
	}
	if _, err := pushResolveURL(mapLookup(map[string]string{})); err == nil {
		t.Fatal("URL resolution succeeded without a URL or port")
	}
}
