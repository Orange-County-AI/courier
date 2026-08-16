package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var HostToolDefs = []ToolDef{
	{
		Name: "read_message",
		Description: "Read the FULL text of an incoming <msg>. The block in your session is only a pointer — a " +
			"one-line preview — so this is the only way to see what was actually sent, including any thread " +
			"context and attachments. Call it before you decide what to do, passing the delivery_id from the " +
			"pointer. Reading tells the host the message reached you; it does not settle it, so you still finish " +
			"with chat_reply or mark_handled.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"delivery_id": map[string]any{"type": "string", "description": "The delivery_id from the incoming <msg>."},
			},
			"required": []string{"delivery_id"},
		},
	},
	{
		Name: "chat_reply",
		Description: "Send your visible response back to the chat application. A human may be reading your terminal, but " +
			"only what you pass to this tool reaches the sender — your plain assistant text does not. Call it at " +
			"most once for each <msg>, passing that message's delivery_id and conversation_id back unchanged. " +
			"For a Mattermost DM only, optional reply_mode chooses the channel root or a thread. A message that " +
			"warrants no reply is finished with mark_handled instead.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"delivery_id":     map[string]any{"type": "string", "description": "The delivery_id from the incoming <msg>."},
				"conversation_id": map[string]any{"type": "string", "description": "The conversation_id from the incoming <msg>."},
				"message":         map[string]any{"type": "string", "description": "The natural-language reply visible to the user."},
				"reply_mode": map[string]any{
					"type": "string", "enum": []string{"root", "thread"},
					"description": "Mattermost DMs only: reply at the DM channel root, or in the incoming/new message thread. Omit to preserve where the message arrived.",
				},
			},
			"required": []string{"delivery_id", "conversation_id", "message"},
		},
	},
	{
		Name: "mark_handled",
		Description: "Confirm a <msg> needs no visible reply — thread chatter that is not addressed to you, an " +
			"automated notice, a duplicate, something you already answered. This is a judgment call and it is " +
			"yours to make: it settles the message exactly as a reply does. Unconfirmed messages are re-delivered " +
			"to you, so use this instead of ignoring one. Pass either the delivery_id or the event_id.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"delivery_id": map[string]any{"type": "string", "description": "The delivery_id from the incoming <msg>."},
				"event_id":    map[string]any{"type": "number", "description": "The event_id, if you have that instead."},
			},
		},
	},
}

var HostToolNames = func() []string {
	names := make([]string, len(HostToolDefs))
	for i, tool := range HostToolDefs {
		names[i] = tool.Name
	}
	return names
}()

type HostToolsOptions struct {
	Store      *Store
	Connectors *Registry
	Shadow     ShadowMode
	Now        func() int64
	Log        func(...any)
}

type HostTools struct {
	store      *Store
	connectors *Registry
	shadow     ShadowMode
	now        func() int64
	log        func(...any)
}

type PostRetryResult struct {
	Posted int `json:"posted"`
	Failed int `json:"failed"`
}

func NewHostTools(opts HostToolsOptions) (*HostTools, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("host tools require a store")
	}
	if opts.Connectors == nil {
		return nil, fmt.Errorf("host tools require a connector registry")
	}
	now := opts.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	log := opts.Log
	if log == nil {
		log = func(...any) {}
	}
	return &HostTools{
		store:      opts.Store,
		connectors: opts.Connectors,
		shadow:     opts.Shadow,
		now:        now,
		log:        log,
	}, nil
}

func stringArg(value any) string {
	if value, ok := value.(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func (h *HostTools) ReadMessage(_ context.Context, args map[string]any) (ToolResult, error) {
	agent := stringArg(args["agent"])
	deliveryID := stringArg(args["delivery_id"])
	if agent == "" {
		return ToolResult{Text: "agent is required — the MCP shim supplies it from CHANNEL_AGENT", IsError: true, Status: 400}, nil
	}
	if deliveryID == "" {
		return ToolResult{Text: "delivery_id is required", IsError: true, Status: 400}, nil
	}

	delivery, err := h.store.GetDelivery(deliveryID)
	if err != nil {
		return ToolResult{}, fmt.Errorf("load delivery %s: %w", deliveryID, err)
	}
	if delivery == nil {
		return ToolResult{Text: "unknown delivery_id: " + deliveryID, IsError: true, Status: 404}, nil
	}
	// A receipt says which agent received the message. Refuse a foreign target
	// before MarkRead or the wrong agent would relax the owner's redelivery.
	if delivery.Target != agent {
		return ToolResult{
			Text:    fmt.Sprintf("delivery %s belongs to %s, not %s", deliveryID, delivery.Target, agent),
			IsError: true,
			Status:  409,
		}, nil
	}

	event, err := h.store.GetEvent(delivery.EventID)
	if err != nil {
		return ToolResult{}, fmt.Errorf("load event %d for delivery %s: %w", delivery.EventID, deliveryID, err)
	}
	if event == nil {
		return ToolResult{Text: fmt.Sprintf("delivery %s has no event", deliveryID), IsError: true, Status: 500}, nil
	}
	first, err := h.store.MarkRead(deliveryID, h.now())
	if err != nil {
		return ToolResult{}, fmt.Errorf("mark delivery %s read: %w", deliveryID, err)
	}
	if first {
		h.log("read receipt for", deliveryID, "— event", event.ID)
	}

	user := "unknown"
	if event.User != nil {
		user = *event.User
	}
	return ToolResult{Text: BuildMsgFull(MsgFullInput{
		DeliveryID:     delivery.ID,
		ConversationID: event.ConversationID,
		User:           user,
		Connector:      event.Connector,
		Status:         string(delivery.Status),
		Content:        event.Content,
		FirstRead:      first,
		Settled:        event.HandledAt != nil,
	})}, nil
}

type postOutcome struct {
	posted    bool
	postError string
}

func (h *HostTools) postAndComplete(ctx context.Context, replyID string) (postOutcome, error) {
	reply, err := h.store.GetReply(replyID)
	if err != nil {
		return postOutcome{}, fmt.Errorf("load reply %s: %w", replyID, err)
	}
	if reply == nil {
		return postOutcome{}, fmt.Errorf("reply %s vanished", replyID)
	}
	if reply.PostedAt != nil {
		if !h.store.CompleteAfterPost(reply.ID, h.now()) {
			return postOutcome{}, fmt.Errorf("posted reply %s could not complete its delivery", reply.ID)
		}
		return postOutcome{posted: true}, nil
	}

	delivery, err := h.store.GetDelivery(reply.DeliveryID)
	if err != nil {
		return postOutcome{}, fmt.Errorf("load delivery %s for reply %s: %w", reply.DeliveryID, reply.ID, err)
	}
	if delivery == nil {
		return postOutcome{}, fmt.Errorf("delivery %s vanished", reply.DeliveryID)
	}
	event, err := h.store.GetEvent(delivery.EventID)
	if err != nil {
		return postOutcome{}, fmt.Errorf("load event %d for reply %s: %w", delivery.EventID, reply.ID, err)
	}
	if event == nil {
		return postOutcome{}, fmt.Errorf("event %d vanished", delivery.EventID)
	}

	connector, postErr := h.connectors.Require(event.Connector)
	if postErr == nil {
		postErr = connector.PostReply(ctx, DeliveryContext{
			Delivery:       *delivery,
			Event:          *event,
			ConversationID: reply.ConversationID,
		}, reply.Message)
	}
	if postErr != nil {
		message := postErr.Error()
		// posted_at stays nil and CompleteAfterPost is never called. RetryPosts
		// owns this reply from here; prompting the agent again would double-post.
		if err := h.store.MarkPostError(reply.ID, message); err != nil {
			return postOutcome{}, fmt.Errorf("record post failure for reply %s: %w", reply.ID, err)
		}
		h.log("post failed for reply", reply.ID, "—", message)
		return postOutcome{postError: message}, nil
	}

	if err := h.store.MarkPosted(reply.ID, h.now()); err != nil {
		return postOutcome{}, fmt.Errorf("mark reply %s posted: %w", reply.ID, err)
	}
	if !h.store.CompleteAfterPost(reply.ID, h.now()) {
		return postOutcome{}, fmt.Errorf("posted reply %s could not complete its delivery", reply.ID)
	}
	return postOutcome{posted: true}, nil
}

func (h *HostTools) ChatReply(ctx context.Context, args map[string]any) (ToolResult, error) {
	// First, before validating or writing anything. A reply row created in
	// shadow mode would leave RetryPosts trying to reach production forever.
	if refused := h.shadow.Refusal("chat_reply"); refused != nil {
		return *refused, nil
	}

	agent := stringArg(args["agent"])
	deliveryID := stringArg(args["delivery_id"])
	conversationID := stringArg(args["conversation_id"])
	message := stringArg(args["message"])
	replyMode := stringArg(args["reply_mode"])
	if agent == "" {
		return ToolResult{Text: "agent is required — the MCP shim supplies it from CHANNEL_AGENT", IsError: true, Status: 400}, nil
	}
	if deliveryID == "" || conversationID == "" || message == "" {
		return ToolResult{Text: "delivery_id, conversation_id, and message are required", IsError: true, Status: 400}, nil
	}

	delivery, err := h.store.GetDelivery(deliveryID)
	if err != nil {
		return ToolResult{}, fmt.Errorf("load delivery %s: %w", deliveryID, err)
	}
	if delivery == nil {
		return ToolResult{Text: "unknown delivery_id: " + deliveryID, IsError: true, Status: 404}, nil
	}
	if delivery.Target != agent {
		return ToolResult{
			Text:    fmt.Sprintf("delivery %s belongs to %s, not %s", deliveryID, delivery.Target, agent),
			IsError: true,
			Status:  409,
		}, nil
	}
	event, err := h.store.GetEvent(delivery.EventID)
	if err != nil {
		return ToolResult{}, fmt.Errorf("load event %d for delivery %s: %w", delivery.EventID, deliveryID, err)
	}
	if event == nil {
		return ToolResult{Text: fmt.Sprintf("delivery %s has no event", deliveryID), IsError: true, Status: 500}, nil
	}
	if event.ConversationID != conversationID {
		return ToolResult{Text: "conversation_id does not match delivery " + deliveryID, IsError: true, Status: 409}, nil
	}
	effectiveConversationID := conversationID
	if replyMode != "" {
		connector, err := h.connectors.Require(event.Connector)
		if err != nil {
			return ToolResult{}, err
		}
		router, ok := connector.(replyRouter)
		if !ok {
			return ToolResult{
				Text:    fmt.Sprintf("reply_mode is not supported by connector %s", event.Connector),
				IsError: true,
				Status:  400,
			}, nil
		}
		effectiveConversationID, err = router.RouteReply(DeliveryContext{
			Delivery: *delivery, Event: *event, ConversationID: conversationID,
		}, replyMode)
		if err != nil {
			return ToolResult{Text: err.Error(), IsError: true, Status: 400}, nil
		}
	}

	reply, duplicate, err := h.store.InsertReply(ReplyInsert{
		DeliveryID:     deliveryID,
		Target:         agent,
		ConversationID: effectiveConversationID,
		Message:        message,
	}, h.now())
	if err != nil {
		return ToolResult{}, fmt.Errorf("insert reply for delivery %s: %w", deliveryID, err)
	}
	if duplicate {
		return ToolResult{Text: "reply was already delivered — not sending it again"}, nil
	}

	outcome, err := h.postAndComplete(ctx, reply.ID)
	if err != nil {
		return ToolResult{}, err
	}
	if !outcome.posted {
		return ToolResult{
			Text: fmt.Sprintf(
				"reply recorded but NOT yet posted to %s: %s. It will be retried automatically; do not call chat_reply again for this delivery.",
				event.Connector,
				outcome.postError,
			),
			IsError: true,
		}, nil
	}
	return ToolResult{Text: "reply delivered to chat"}, nil
}

func eventIDArg(value any) (*int64, bool) {
	var number float64
	switch value := value.(type) {
	case int:
		id := int64(value)
		return &id, true
	case int64:
		id := value
		return &id, true
	case int32:
		id := int64(value)
		return &id, true
	case uint:
		if uint64(value) > math.MaxInt64 {
			id := int64(0)
			return &id, true
		}
		id := int64(value)
		return &id, true
	case uint64:
		if value > math.MaxInt64 {
			id := int64(0)
			return &id, true
		}
		id := int64(value)
		return &id, true
	case float64:
		number = value
	case float32:
		number = float64(value)
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return nil, false
		}
		number = parsed
	case string:
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			integer, intErr := strconv.ParseInt(trimmed, 0, 64)
			if intErr != nil {
				return nil, false
			}
			return &integer, true
		}
		number = parsed
	default:
		return nil, false
	}

	// JavaScript passes any finite number to the store. A fractional or
	// out-of-range value cannot identify an integer SQLite id, but it was still
	// supplied, so preserve the non-match rather than changing it into a 400.
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, false
	}
	if number != math.Trunc(number) || number < math.MinInt64 || number > math.MaxInt64 {
		id := int64(0)
		return &id, true
	}
	id := int64(number)
	return &id, true
}

func (h *HostTools) MarkHandled(_ context.Context, args map[string]any) (ToolResult, error) {
	deliveryID := stringArg(args["delivery_id"])
	eventID, eventIDSupplied := eventIDArg(args["event_id"])
	if deliveryID == "" && !eventIDSupplied {
		return ToolResult{Text: "delivery_id or event_id is required", IsError: true, Status: 400}, nil
	}

	ok := h.store.MarkHandled(MarkHandledArgs{DeliveryID: deliveryID, EventID: eventID}, h.now())
	if !ok {
		identity := deliveryID
		if identity == "" {
			identity = fmt.Sprintf("event %d", *eventID)
		}
		return ToolResult{Text: fmt.Sprintf("nothing to mark handled for %s (already handled, or unknown)", identity)}, nil
	}
	return ToolResult{Text: "marked handled — it will not be re-delivered"}, nil
}

func (h *HostTools) CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
	switch name {
	case "read_message":
		return h.ReadMessage(ctx, args)
	case "chat_reply":
		return h.ChatReply(ctx, args)
	case "mark_handled":
		return h.MarkHandled(ctx, args)
	default:
		return ToolResult{}, fmt.Errorf("unknown host tool: %s", name)
	}
}

// RetryPosts is the only path that retries a recorded outbound reply. The
// agent already did its part; prompting it or accepting another chat_reply
// would make the number of posts depend on retry timing.
func (h *HostTools) RetryPosts(ctx context.Context) (PostRetryResult, error) {
	if h.shadow.Enabled {
		return PostRetryResult{}, nil
	}
	replies, err := h.store.UnpostedReplies()
	if err != nil {
		return PostRetryResult{}, fmt.Errorf("load unposted replies: %w", err)
	}
	var result PostRetryResult
	for _, reply := range replies {
		outcome, err := h.postAndComplete(ctx, reply.ID)
		if err != nil {
			return result, err
		}
		if outcome.posted {
			result.Posted++
		} else {
			result.Failed++
		}
	}
	if result.Posted > 0 || result.Failed > 0 {
		h.log("post retry sweep:", result.Posted, "posted,", result.Failed, "still failing")
	}
	return result, nil
}
