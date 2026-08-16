package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type hosttoolsPost struct {
	Context DeliveryContext
	Message string
}

type hosttoolsFakeConnector struct {
	name         string
	tools        []ToolDef
	instructions string
	postErr      error
	postAttempts int
	posts        []hosttoolsPost
	onPost       func(DeliveryContext, string)
	toolResult   ToolResult
	toolErr      error
}

func (c *hosttoolsFakeConnector) Name() string             { return c.name }
func (c *hosttoolsFakeConnector) ManifestTools() []ToolDef { return append([]ToolDef(nil), c.tools...) }
func (c *hosttoolsFakeConnector) Instructions() string     { return c.instructions }
func (c *hosttoolsFakeConnector) Start(context.Context) error {
	return nil
}
func (c *hosttoolsFakeConnector) Stop(context.Context) error { return nil }
func (c *hosttoolsFakeConnector) CallTool(context.Context, string, map[string]any) (ToolResult, error) {
	return c.toolResult, c.toolErr
}
func (c *hosttoolsFakeConnector) PostReply(_ context.Context, dc DeliveryContext, message string) error {
	c.postAttempts++
	if c.onPost != nil {
		c.onPost(dc, message)
	}
	if c.postErr != nil {
		return c.postErr
	}
	c.posts = append(c.posts, hosttoolsPost{Context: dc, Message: message})
	return nil
}

type hosttoolsHarness struct {
	t         *testing.T
	store     *Store
	registry  *Registry
	connector *hosttoolsFakeConnector
	tools     *HostTools
	now       int64
	target    string
}

func newHosttoolsHarness(t *testing.T, shadow bool) *hosttoolsHarness {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	connector := &hosttoolsFakeConnector{
		name: "testconn",
		tools: []ToolDef{{
			Name:        "test_action",
			Description: "test connector action",
			InputSchema: map[string]any{"type": "object"},
		}},
		instructions: "Test connector instructions.",
		toolResult:   ToolResult{Text: "ok"},
	}
	registry := NewRegistry()
	if err := registry.Register(connector); err != nil {
		t.Fatal(err)
	}
	h := &hosttoolsHarness{
		t:         t,
		store:     store,
		registry:  registry,
		connector: connector,
		now:       1_000_000,
		target:    "agent-one",
	}
	h.tools, err = NewHostTools(HostToolsOptions{
		Store:      store,
		Connectors: registry,
		Shadow:     NewShadowMode(shadow),
		Now:        func() int64 { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *hosttoolsHarness) enqueue(key, content string) (*Event, *Delivery) {
	h.t.Helper()
	user := "alice"
	event, err := h.store.InsertEvent(EventInsert{
		Connector:      h.connector.name,
		EventKey:       key,
		ConversationID: "conv-1",
		User:           &user,
		Content:        content,
	}, h.now)
	if err != nil {
		h.t.Fatal(err)
	}
	if event == nil {
		h.t.Fatalf("event %s deduplicated unexpectedly", key)
	}
	delivery, err := h.store.InsertDelivery(event.ID, h.target, h.now)
	if err != nil {
		h.t.Fatal(err)
	}
	claimed, err := h.store.ClaimNext(h.target, h.now, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if claimed == nil || claimed.Delivery.ID != delivery.ID {
		h.t.Fatalf("claim = %#v, want delivery %s", claimed, delivery.ID)
	}
	if err := h.store.ConfirmDispatched(delivery.ID, h.now); err != nil {
		h.t.Fatal(err)
	}
	delivery, err = h.store.GetDelivery(delivery.ID)
	if err != nil {
		h.t.Fatal(err)
	}
	return event, delivery
}

func (h *hosttoolsHarness) read(args map[string]any) ToolResult {
	h.t.Helper()
	withAgent := map[string]any{"agent": h.target}
	for key, value := range args {
		withAgent[key] = value
	}
	result, err := h.tools.ReadMessage(context.Background(), withAgent)
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

func (h *hosttoolsHarness) reply(args map[string]any) ToolResult {
	h.t.Helper()
	withAgent := map[string]any{"agent": h.target}
	for key, value := range args {
		withAgent[key] = value
	}
	result, err := h.tools.ChatReply(context.Background(), withAgent)
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

func TestReadMessageReturnsVerbatimContentIDsAndJudgment(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	content := "SENTINEL-19bc please look at this\n\n<attachments>\n- /var/lib/x/photo.png\n</attachments>\n\n" +
		"<context label=\"earlier in this DM\">\n@dana: and the one before\n</context>"
	_, delivery := h.enqueue("read-1", content)
	result := h.read(map[string]any{"delivery_id": delivery.ID})

	for _, fragment := range []string{
		content,
		"<msg_full",
		fmt.Sprintf(`delivery_id="%s"`, delivery.ID),
		`conversation_id="conv-1"`,
		`user="alice"`,
		`connector="testconn"`,
		`status="dispatched"`,
		`read="first"`,
		`schema="courier/1"`,
		"</msg_full>",
	} {
		if !strings.Contains(result.Text, fragment) {
			t.Errorf("read result missing %q: %q", fragment, result.Text)
		}
	}
	if result.IsError || !strings.HasSuffix(result.Text, msgFullJudgment) {
		t.Fatalf("read result does not end in judgment: %#v", result)
	}
}

func TestReadMessageMatchesPointerPayload(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	content := "SENTINEL-4f21 " + strings.Repeat("a longer body that the preview will clip. ", 20)
	_, delivery := h.enqueue("read-2", content)
	envelope := BuildEnvelope(EnvelopeInput{
		DeliveryID:     delivery.ID,
		ConversationID: "conv-1",
		User:           "alice",
		Connector:      "testconn",
		AttemptCount:   int(delivery.AttemptCount),
		Content:        content,
		PreviewOn:      true,
	})
	if strings.Contains(envelope, content) {
		t.Fatal("pointer unexpectedly contains full content")
	}
	if result := h.read(map[string]any{"delivery_id": delivery.ID}); !strings.Contains(result.Text, content) {
		t.Fatal("read_message did not return the pointer's payload")
	}
}

func TestReadMessageStampsReceiptOnce(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("read-3", "hi")
	first := h.read(map[string]any{"delivery_id": delivery.ID})
	stored, err := h.store.GetDelivery(delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ReadAt == nil || !strings.Contains(first.Text, `read="first"`) {
		t.Fatalf("first read did not stamp: %#v, %q", stored, first.Text)
	}
	at := *stored.ReadAt
	h.now += 60_000
	again := h.read(map[string]any{"delivery_id": delivery.ID})
	stored, err = h.store.GetDelivery(delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ReadAt == nil || *stored.ReadAt != at || !strings.Contains(again.Text, `read="again"`) {
		t.Fatalf("second read moved receipt: %#v, %q", stored, again.Text)
	}
}

func TestReadMessageSettlesNothing(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("read-4", "hi")
	h.read(map[string]any{"delivery_id": delivery.ID})
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	if storedDelivery.Status != DeliveryDispatched || storedEvent.HandledAt != nil {
		t.Fatalf("reading settled rows: delivery=%#v event=%#v", storedDelivery, storedEvent)
	}
}

func TestReadMessageRequiresAgentAndDeliveryID(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	noID := h.read(map[string]any{})
	if noID.Status != 400 || !noID.IsError || !strings.Contains(noID.Text, "delivery_id is required") {
		t.Fatalf("no-id result = %#v", noID)
	}
	noAgent, err := h.tools.ReadMessage(context.Background(), map[string]any{"delivery_id": "anything"})
	if err != nil {
		t.Fatal(err)
	}
	if noAgent.Status != 400 || !strings.Contains(noAgent.Text, "agent is required") {
		t.Fatalf("no-agent result = %#v", noAgent)
	}
}

func TestReadMessageUnknownDeliveryIs404(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	result := h.read(map[string]any{"delivery_id": "not-a-real-id"})
	if result.Status != 404 || !result.IsError || !strings.Contains(result.Text, "unknown delivery_id") {
		t.Fatalf("unknown result = %#v", result)
	}
}

func TestReadMessageForeignTarget409BeforeStamp(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("read-7", "a private message")
	result, err := h.tools.ReadMessage(context.Background(), map[string]any{
		"agent":       "imposter",
		"delivery_id": delivery.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := h.store.GetDelivery(delivery.ID)
	if result.Status != 409 || !result.IsError || !strings.Contains(result.Text, "belongs to "+h.target) {
		t.Fatalf("foreign read result = %#v", result)
	}
	if strings.Contains(result.Text, "a private message") || stored.ReadAt != nil {
		t.Fatalf("foreign read leaked or stamped: result=%#v delivery=%#v", result, stored)
	}
}

func TestReadMessageHandledDeliveryStaysReadable(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("read-8", "what did I answer here")
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "answered"})
	result := h.read(map[string]any{"delivery_id": delivery.ID})
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	if !strings.Contains(result.Text, "what did I answer here") || !strings.Contains(result.Text, `status="handled"`) || !strings.Contains(result.Text, "already settled") {
		t.Fatalf("settled history result = %q", result.Text)
	}
	if strings.Contains(result.Text, msgFullJudgment) || storedDelivery.Status != DeliveryHandled || storedEvent.HandledAt == nil || len(h.connector.posts) != 1 {
		t.Fatalf("history read changed settlement: delivery=%#v event=%#v posts=%d", storedDelivery, storedEvent, len(h.connector.posts))
	}
}

func TestReadMessageMarkedHandledDeliveryStaysReadable(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("read-9", "thread chatter")
	h.read(map[string]any{"delivery_id": delivery.ID})
	if result, err := h.tools.MarkHandled(context.Background(), map[string]any{"delivery_id": delivery.ID}); err != nil || result.IsError {
		t.Fatalf("mark handled = %#v, %v", result, err)
	}
	result := h.read(map[string]any{"delivery_id": delivery.ID})
	if !strings.Contains(result.Text, "thread chatter") || !strings.Contains(result.Text, "already settled") {
		t.Fatalf("marked history result = %q", result.Text)
	}
}

func TestPointerReadReplyRoundTrip(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	content := "SENTINEL-c30d can you look at the deploy? " + strings.Repeat("Here is more context. ", 20) + "actual question at the END"
	event, delivery := h.enqueue("read-10", content)
	envelope := BuildEnvelope(EnvelopeInput{DeliveryID: delivery.ID, ConversationID: "conv-1", User: "alice", Connector: "testconn", AttemptCount: 1, Content: content, PreviewOn: true})
	if strings.Contains(envelope, "actual question at the END") || !strings.Contains(strings.Split(envelope, "\n")[0], fmt.Sprintf(`delivery_id="%s"`, delivery.ID)) {
		t.Fatalf("invalid pointer: %q", envelope)
	}
	if full := h.read(map[string]any{"delivery_id": delivery.ID}); !strings.Contains(full.Text, content) {
		t.Fatal("full message missing content")
	}
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "looking now"})
	storedEvent, _ := h.store.GetEvent(event.ID)
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	if len(h.connector.posts) != 1 || storedEvent.HandledAt == nil || storedDelivery.ReadAt == nil {
		t.Fatalf("round trip incomplete: posts=%d event=%#v delivery=%#v", len(h.connector.posts), storedEvent, storedDelivery)
	}
}

func TestPointerReadMarkHandledRoundTrip(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("read-11", "just noting for the humans")
	h.read(map[string]any{"delivery_id": delivery.ID})
	if _, err := h.tools.MarkHandled(context.Background(), map[string]any{"delivery_id": delivery.ID}); err != nil {
		t.Fatal(err)
	}
	reply, err := h.store.GetReplyByDelivery(delivery.ID, h.target)
	if err != nil {
		t.Fatal(err)
	}
	storedEvent, _ := h.store.GetEvent(event.ID)
	reclaimed, err := h.store.SweepStuckDispatches(h.now+3_600_000, h.target)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.connector.posts) != 0 || reply != nil || storedEvent.HandledAt == nil || len(reclaimed) != 0 {
		t.Fatalf("mark round trip incomplete: posts=%d reply=%#v event=%#v reclaimed=%v", len(h.connector.posts), reply, storedEvent, reclaimed)
	}
}

func TestChatReplyPostsThenCompletes(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("reply-1", "hello?")
	h.connector.onPost = func(_ DeliveryContext, message string) {
		if message != "hi there" {
			t.Errorf("posted message = %q", message)
		}
		reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
		storedDelivery, _ := h.store.GetDelivery(delivery.ID)
		storedEvent, _ := h.store.GetEvent(event.ID)
		if reply == nil || reply.PostedAt != nil || storedDelivery.Status != DeliveryReplied || storedEvent.HandledAt != nil {
			t.Errorf("post ran after premature completion: reply=%#v delivery=%#v event=%#v", reply, storedDelivery, storedEvent)
		}
	}
	result := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi there"})
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
	if result.IsError || len(h.connector.posts) != 1 || reply.PostedAt == nil || storedDelivery.Status != DeliveryHandled || storedEvent.HandledAt == nil {
		t.Fatalf("clean reply incomplete: result=%#v reply=%#v delivery=%#v event=%#v", result, reply, storedDelivery, storedEvent)
	}
}

func TestChatReplyHandledDeliveryNotClaimableOrSweepable(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("reply-2", "hello?")
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi"})
	reclaimed, err := h.store.SweepStuckDispatches(h.now+60_000, h.target)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := h.store.ClaimNext(h.target, h.now+60_000, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 0 || claimed != nil {
		t.Fatalf("handled delivery returned: reclaimed=%v claimed=%#v", reclaimed, claimed)
	}
}

func TestDuplicateChatReplyPostsExactlyOnce(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("reply-3", "hello?")
	first := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi there"})
	second := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi there"})
	third := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "a DIFFERENT message"})
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
	if first.Text != "reply delivered to chat" || !strings.Contains(second.Text, "already delivered") || !strings.Contains(third.Text, "already delivered") {
		t.Fatalf("duplicate outcomes: %#v %#v %#v", first, second, third)
	}
	if len(h.connector.posts) != 1 || h.connector.posts[0].Message != "hi there" || reply.Message != "hi there" {
		t.Fatalf("duplicate posted or overwrote: posts=%#v reply=%#v", h.connector.posts, reply)
	}
}

func TestReplyTableOneRowPerDeliveryTarget(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("reply-4", "hello?")
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "one"})
	reply, duplicate, err := h.store.InsertReply(ReplyInsert{DeliveryID: delivery.ID, Target: h.target, ConversationID: "conv-1", Message: "two"}, h.now)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate || reply.Message != "one" {
		t.Fatalf("duplicate insert = %#v, duplicate=%v", reply, duplicate)
	}
}

func TestChatReplyForeignTarget409ChangesNothing(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("reply-5", "hello?")
	before, _ := h.store.GetDelivery(delivery.ID)
	result, err := h.tools.ChatReply(context.Background(), map[string]any{
		"agent": "some-other-agent", "delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "not mine",
	})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := h.store.GetDelivery(delivery.ID)
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, "some-other-agent")
	storedEvent, _ := h.store.GetEvent(event.ID)
	if result.Status != 409 || !result.IsError || len(h.connector.posts) != 0 || reply != nil || !reflect.DeepEqual(before, after) || storedEvent.HandledAt != nil {
		t.Fatalf("foreign reply changed state: result=%#v before=%#v after=%#v reply=%#v", result, before, after, reply)
	}
}

func TestChatReplyConversationMismatch409ChangesNothing(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("reply-6", "hello?")
	before, _ := h.store.GetDelivery(delivery.ID)
	result := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "wrong", "message": "hi"})
	after, _ := h.store.GetDelivery(delivery.ID)
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
	storedEvent, _ := h.store.GetEvent(event.ID)
	if result.Status != 409 || !result.IsError || !strings.Contains(result.Text, "conversation_id does not match") || len(h.connector.posts) != 0 || reply != nil || !reflect.DeepEqual(before, after) || storedEvent.HandledAt != nil {
		t.Fatalf("conversation mismatch changed state: result=%#v before=%#v after=%#v", result, before, after)
	}
}

func TestChatReplyUnknownDeliveryIs404(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	result := h.reply(map[string]any{"delivery_id": "no-such-delivery", "conversation_id": "conv-1", "message": "hi"})
	if result.Status != 404 || !result.IsError {
		t.Fatalf("unknown reply result = %#v", result)
	}
}

func TestFailedPostStaysUnsettledUntilRetry(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("reply-8", "hello?")
	h.connector.postErr = errors.New("mattermost returned 503")
	result := h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi there"})
	reply, _ := h.store.GetReplyByDelivery(delivery.ID, h.target)
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	if !result.IsError || !strings.Contains(result.Text, "NOT yet posted") || !strings.Contains(result.Text, "do not call chat_reply again") {
		t.Fatalf("post failure result = %#v", result)
	}
	if len(h.connector.posts) != 0 || storedDelivery.Status != DeliveryReplied || reply.PostedAt != nil || reply.PostError == nil || !strings.Contains(*reply.PostError, "503") || storedEvent.HandledAt != nil {
		t.Fatalf("post failure settled or vanished: reply=%#v delivery=%#v event=%#v", reply, storedDelivery, storedEvent)
	}
	failed, err := h.tools.RetryPosts(context.Background())
	if err != nil || failed != (PostRetryResult{Failed: 1}) {
		t.Fatalf("failed retry = %#v, %v", failed, err)
	}
	storedEvent, _ = h.store.GetEvent(event.ID)
	if storedEvent.HandledAt != nil {
		t.Fatal("failed retry settled event")
	}
	h.connector.postErr = nil
	posted, err := h.tools.RetryPosts(context.Background())
	if err != nil || posted != (PostRetryResult{Posted: 1}) {
		t.Fatalf("successful retry = %#v, %v", posted, err)
	}
	reply, _ = h.store.GetReplyByDelivery(delivery.ID, h.target)
	storedDelivery, _ = h.store.GetDelivery(delivery.ID)
	storedEvent, _ = h.store.GetEvent(event.ID)
	if len(h.connector.posts) != 1 || reply.PostedAt == nil || storedDelivery.Status != DeliveryHandled || storedEvent.HandledAt == nil {
		t.Fatalf("retry did not complete: reply=%#v delivery=%#v event=%#v posts=%d", reply, storedDelivery, storedEvent, len(h.connector.posts))
	}
	noop, err := h.tools.RetryPosts(context.Background())
	if err != nil || noop != (PostRetryResult{}) || len(h.connector.posts) != 1 {
		t.Fatalf("retry not idempotent: %#v, %v posts=%d", noop, err, len(h.connector.posts))
	}
}

func TestDuplicateChatReplyDuringFailureDoesNotDoublePost(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	_, delivery := h.enqueue("reply-9", "hello?")
	h.connector.postErr = errors.New("down")
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi"})
	h.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi"})
	h.connector.postErr = nil
	if _, err := h.tools.RetryPosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.connector.posts) != 1 {
		t.Fatalf("posts = %d, want 1", len(h.connector.posts))
	}
}

func TestMarkHandledEndsDeliveryWithoutPost(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, delivery := h.enqueue("reply-10", "bot notice")
	result, err := h.tools.MarkHandled(context.Background(), map[string]any{"delivery_id": delivery.ID})
	if err != nil {
		t.Fatal(err)
	}
	storedDelivery, _ := h.store.GetDelivery(delivery.ID)
	storedEvent, _ := h.store.GetEvent(event.ID)
	reclaimed, _ := h.store.SweepStuckDispatches(h.now+60_000, h.target)
	if result.IsError || len(h.connector.posts) != 0 || storedDelivery.Status != DeliveryHandled || storedEvent.HandledAt == nil || len(reclaimed) != 0 {
		t.Fatalf("mark handled incomplete: result=%#v delivery=%#v event=%#v reclaimed=%v", result, storedDelivery, storedEvent, reclaimed)
	}
}

func TestMarkHandledAcceptsEventIDAndIsIdempotent(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, _ := h.enqueue("reply-11", "bot notice")
	first, err := h.tools.MarkHandled(context.Background(), map[string]any{"event_id": float64(event.ID)})
	if err != nil || first.IsError {
		t.Fatalf("first mark = %#v, %v", first, err)
	}
	second, err := h.tools.MarkHandled(context.Background(), map[string]any{"event_id": fmt.Sprint(event.ID)})
	if err != nil || second.IsError || !strings.Contains(second.Text, "already handled") {
		t.Fatalf("second mark = %#v, %v", second, err)
	}
}

func TestMarkHandledRequiresAnID(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	result, err := h.tools.MarkHandled(context.Background(), map[string]any{})
	if err != nil || result.Status != 400 || !result.IsError {
		t.Fatalf("empty mark = %#v, %v", result, err)
	}
}

func TestDuplicateInboundEventInsertsOnce(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	first, err := h.store.InsertEvent(EventInsert{Connector: "testconn", EventKey: "post-123", ConversationID: "conv-1", Content: "hello"}, h.now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.store.InsertEvent(EventInsert{Connector: "testconn", EventKey: "post-123", ConversationID: "conv-1", Content: "hello"}, h.now)
	if err != nil {
		t.Fatal(err)
	}
	count, _ := h.store.CountEvents()
	if first == nil || second != nil || count != 1 {
		t.Fatalf("dedupe first=%#v second=%#v count=%d", first, second, count)
	}
}

func TestTwoConnectorsMayShareEventKey(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	for _, event := range []EventInsert{
		{Connector: "a", EventKey: "k", ConversationID: "c", Content: "x"},
		{Connector: "b", EventKey: "k", ConversationID: "c", Content: "y"},
	} {
		if inserted, err := h.store.InsertEvent(event, h.now); err != nil || inserted == nil {
			t.Fatalf("insert %#v = %#v, %v", event, inserted, err)
		}
	}
	count, _ := h.store.CountEvents()
	if count != 2 {
		t.Fatalf("event count = %d", count)
	}
}

func TestEventGetsOneOpenDeliveryUntilFailure(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	event, err := h.store.InsertEvent(EventInsert{Connector: "testconn", EventKey: "one-open", ConversationID: "c", Content: "x"}, h.now)
	if err != nil {
		t.Fatal(err)
	}
	a, err := h.store.InsertDelivery(event.ID, h.target, h.now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.store.InsertDelivery(event.ID, h.target, h.now)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatalf("duplicate open deliveries: %s != %s", a.ID, b.ID)
	}
	if err := h.store.FailDelivery(a.ID, "giving up"); err != nil {
		t.Fatal(err)
	}
	c, err := h.store.InsertDelivery(event.ID, "someone-else", h.now)
	if err != nil {
		t.Fatal(err)
	}
	if c.ID == a.ID {
		t.Fatal("failed delivery was reused")
	}
}

func TestChatReplyShadowRefusalComesBeforeValidationAndWrites(t *testing.T) {
	h := newHosttoolsHarness(t, true)
	result, err := h.tools.ChatReply(context.Background(), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	replies, storeErr := h.store.UnpostedReplies()
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if result.Status != 503 || !result.IsError || !strings.Contains(result.Text, ShadowRefusal) || len(replies) != 0 || h.connector.postAttempts != 0 {
		t.Fatalf("shadow reply = %#v replies=%v attempts=%d", result, replies, h.connector.postAttempts)
	}
}

func TestRetryPostsIsShadowGated(t *testing.T) {
	live := newHosttoolsHarness(t, false)
	_, delivery := live.enqueue("shadow-retry-source", "hello")
	live.connector.postErr = errors.New("down")
	live.reply(map[string]any{"delivery_id": delivery.ID, "conversation_id": "conv-1", "message": "hi"})
	shadowTools, err := NewHostTools(HostToolsOptions{Store: live.store, Connectors: live.registry, Shadow: NewShadowMode(true), Now: func() int64 { return live.now }})
	if err != nil {
		t.Fatal(err)
	}
	live.connector.postErr = nil
	result, err := shadowTools.RetryPosts(context.Background())
	if err != nil || result != (PostRetryResult{}) || live.connector.postAttempts != 1 {
		t.Fatalf("shadow retry = %#v, %v attempts=%d", result, err, live.connector.postAttempts)
	}
}
