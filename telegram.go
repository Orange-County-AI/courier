package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	TelegramName              = "telegram"
	telegramWatermarkKey      = "telegram:update_id"
	telegramMaxWebhookBytes   = 8 << 20
	telegramTypingRefresh     = 4 * time.Second
	telegramTypingMaximum     = 5 * time.Minute
	telegramMessageUTF16Limit = 4096
)

var telegramTools = []ToolDef{
	{
		Name:        "telegram_react",
		Description: "Add or clear an emoji reaction on a Telegram message. Use the event's conversation_id unchanged so forum-topic routing stays exact.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"conversation_id": map[string]any{"type": "string", "description": "The Telegram event's conversation_id, unchanged."},
				"message_id":      map[string]any{"type": "string", "description": "The Telegram message id to react to."},
				"emoji":           map[string]any{"type": "string", "description": "Reaction emoji, or empty to clear."},
			},
			"required": []string{"conversation_id", "message_id", "emoji"},
		},
	},
	{
		Name:        "telegram_edit_message",
		Description: "Edit text of a Telegram message this bot previously sent. Edits do not push-notify; send a fresh chat_reply when a long task completes.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"conversation_id": map[string]any{"type": "string", "description": "The Telegram event's conversation_id, unchanged."},
				"message_id":      map[string]any{"type": "string", "description": "The bot-authored Telegram message id to edit."},
				"text":            map[string]any{"type": "string", "description": "Replacement message text."},
			},
			"required": []string{"conversation_id", "message_id", "text"},
		},
	},
}

const telegramInstructions = "Telegram messages arrive with connector=\"telegram\". conversation_id is the chat id, or " +
	"`<chat_id>:<message_thread_id>` inside a forum topic; pass it back unchanged. chat_reply sends to that exact chat/topic. " +
	"Downloaded media is listed by local path in the message content. telegram_react and telegram_edit_message are only for " +
	"reactions and edits; use chat_reply for the ordinary answer."

const telegramVisibleAckInstructions = " This deployment requires a visible acknowledgement for every Telegram message. " +
	"Use chat_reply for requests and a brief acknowledgement for FYI messages; do not silently settle Telegram with mark_handled."

type TelegramConnectorConfig struct {
	Store   *Store
	Target  string
	Options TelegramOptions
	Shadow  ShadowMode
	Client  telegramAPI
	Now     func() int64
	Log     func(string, ...any)

	TypingRefresh time.Duration
	TypingMaximum time.Duration
}

type TelegramConnector struct {
	store  *Store
	target string
	opts   TelegramOptions
	shadow ShadowMode
	client telegramAPI
	now    func() int64
	log    func(string, ...any)

	allowedUsers map[string]struct{}
	allowedChats map[string]struct{}

	typingRefresh time.Duration
	typingMaximum time.Duration
	typingMu      sync.Mutex
	typing        map[string]context.CancelFunc
	typingWG      sync.WaitGroup

	serverMu sync.Mutex
	server   *http.Server
	listener net.Listener
}

type TelegramUser struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
}

type TelegramChat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

type TelegramPhoto struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	Width        int64  `json:"width"`
	Height       int64  `json:"height"`
}

type TelegramMedia struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	MIMEType     string `json:"mime_type"`
	FileName     string `json:"file_name"`
}

type TelegramEntity struct {
	Type   string        `json:"type"`
	Offset int           `json:"offset"`
	Length int           `json:"length"`
	User   *TelegramUser `json:"user,omitempty"`
}

type TelegramMessage struct {
	MessageID       int64            `json:"message_id"`
	MessageThreadID int64            `json:"message_thread_id"`
	Date            int64            `json:"date"`
	From            *TelegramUser    `json:"from"`
	Chat            TelegramChat     `json:"chat"`
	Text            string           `json:"text"`
	Caption         string           `json:"caption"`
	Entities        []TelegramEntity `json:"entities"`
	CaptionEntities []TelegramEntity `json:"caption_entities"`
	Photo           []TelegramPhoto  `json:"photo"`
	Document        *TelegramMedia   `json:"document"`
	Voice           *TelegramMedia   `json:"voice"`
	Video           *TelegramMedia   `json:"video"`
	Audio           *TelegramMedia   `json:"audio"`
	ReplyToMessage  *TelegramMessage `json:"reply_to_message"`
}

type TelegramUpdate struct {
	UpdateID      int64            `json:"update_id"`
	Message       *TelegramMessage `json:"message"`
	EditedMessage *TelegramMessage `json:"edited_message"`
}

type telegramAttachment struct {
	FileID   string
	Name     string
	MIMEType string
}

func NewTelegramConnector(config TelegramConnectorConfig) (*TelegramConnector, error) {
	if config.Store == nil {
		return nil, errors.New("telegram connector requires a store")
	}
	if strings.TrimSpace(config.Target) == "" {
		return nil, errors.New("telegram connector requires a target")
	}
	missing := make([]string, 0, 3)
	if strings.TrimSpace(config.Options.BotToken) == "" {
		missing = append(missing, "TELEGRAM_BOT_TOKEN")
	}
	if strings.TrimSpace(config.Options.WebhookSecret) == "" {
		missing = append(missing, "TELEGRAM_WEBHOOK_SECRET")
	}
	if config.Options.ListenPort < 0 || config.Options.ListenPort > 65535 {
		return nil, errors.New("TELEGRAM_LISTEN_PORT must be from 1 through 65535")
	}
	if config.Options.ListenPort == 0 && config.Client == nil {
		missing = append(missing, "TELEGRAM_LISTEN_PORT")
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("telegram connector is partially configured — missing %s", strings.Join(missing, ", "))
	}
	allowedUsers := telegramIDSet(config.Options.AllowedUserIDs)
	allowedChats := telegramIDSet(config.Options.AllowedChatIDs)
	if len(allowedUsers) == 0 && len(allowedChats) == 0 {
		return nil, errors.New("telegram connector requires TELEGRAM_ALLOWED_USER_IDS or TELEGRAM_ALLOWED_CHAT_IDS")
	}
	client := config.Client
	if client == nil {
		created, err := NewTelegramClient(TelegramClientConfig{
			Token: config.Options.BotToken, AttachmentDir: config.Options.AttachmentDir,
		})
		if err != nil {
			return nil, err
		}
		client = created
	}
	now := config.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	log := config.Log
	if log == nil {
		log = func(string, ...any) {}
	}
	refresh := config.TypingRefresh
	if refresh <= 0 {
		refresh = telegramTypingRefresh
	}
	maximum := config.TypingMaximum
	if maximum <= 0 {
		maximum = telegramTypingMaximum
	}
	return &TelegramConnector{
		store: config.Store, target: strings.TrimSpace(config.Target), opts: config.Options,
		shadow: config.Shadow, client: client, now: now, log: log,
		allowedUsers: allowedUsers, allowedChats: allowedChats,
		typingRefresh: refresh, typingMaximum: maximum, typing: make(map[string]context.CancelFunc),
	}, nil
}

func (t *TelegramConnector) Name() string { return TelegramName }

func (t *TelegramConnector) ManifestTools() []ToolDef {
	return append([]ToolDef(nil), telegramTools...)
}

func (t *TelegramConnector) Instructions() string {
	if t.opts.RequireVisibleAck {
		return telegramInstructions + telegramVisibleAckInstructions
	}
	return telegramInstructions
}

func (t *TelegramConnector) CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
	if name != "telegram_react" && name != "telegram_edit_message" {
		return ToolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
	if refusal := t.shadow.Refusal(name); refusal != nil {
		return *refusal, nil
	}
	conversationID, _ := args["conversation_id"].(string)
	messageIDRaw, _ := args["message_id"].(string)
	chatID, _, err := TelegramDecomposeConversationID(conversationID)
	if err != nil {
		return ToolResult{Status: 400, Text: err.Error(), IsError: true}, nil
	}
	messageID, err := strconv.ParseInt(strings.TrimSpace(messageIDRaw), 10, 64)
	if err != nil || messageID <= 0 {
		return ToolResult{Status: 400, Text: name + " requires a positive message_id", IsError: true}, nil
	}
	switch name {
	case "telegram_react":
		emoji, ok := args["emoji"].(string)
		if !ok {
			return ToolResult{Status: 400, Text: "telegram_react requires emoji (empty clears it)", IsError: true}, nil
		}
		if err := t.client.SetReaction(ctx, chatID, messageID, emoji); err != nil {
			return ToolResult{}, t.outboundError("react", err)
		}
		t.stopTyping(conversationID)
		if emoji == "" {
			return ToolResult{Text: "cleared reactions"}, nil
		}
		return ToolResult{Text: "reacted " + emoji}, nil
	case "telegram_edit_message":
		text, _ := args["text"].(string)
		if strings.TrimSpace(text) == "" {
			return ToolResult{Status: 400, Text: "telegram_edit_message requires non-empty text", IsError: true}, nil
		}
		if err := t.client.EditMessageText(ctx, chatID, messageID, text); err != nil {
			return ToolResult{}, t.outboundError("edit", err)
		}
		return ToolResult{Text: "edited"}, nil
	}
	panic("unreachable")
}

// PostReply returns nil only after Telegram has confirmed every chunk with a
// message_id. A partial multi-chunk failure remains unsettled rather than
// claiming the human saw a complete answer.
func (t *TelegramConnector) PostReply(ctx context.Context, delivery DeliveryContext, message string) error {
	if err := t.shadow.Refuse("posting to Telegram"); err != nil {
		return err
	}
	if strings.TrimSpace(message) == "" {
		return errors.New("refusing to post an empty message to Telegram")
	}
	chatID, threadID, err := TelegramDecomposeConversationID(delivery.ConversationID)
	if err != nil {
		return err
	}
	for _, chunk := range telegramSplitText(message) {
		if _, err := t.client.SendMessage(ctx, telegramSendMessage{ChatID: chatID, MessageThreadID: threadID, Text: chunk}); err != nil {
			return t.outboundError("send", err)
		}
	}
	t.stopTyping(delivery.ConversationID)
	return nil
}

// NotifyUnavailable sends a disconnect notice without
// settling the delivery. The sync-state marker is written before the outbound
// call: Telegram has no idempotency key, so at-most-one visible attempt is
// safer than duplicate warnings after a crash or restart.
func (t *TelegramConnector) NotifyUnavailable(ctx context.Context, delivery DeliveryContext) error {
	notice := strings.TrimSpace(t.opts.DisconnectNotice)
	if notice == "" || t.shadow.Suppressed() {
		return nil
	}
	key := "telegram:disconnect_notice:" + delivery.Delivery.ID
	marker, err := t.store.GetSyncState(key)
	if err != nil {
		return fmt.Errorf("load Telegram disconnect notice marker: %w", err)
	}
	if marker != nil {
		return nil
	}
	if err := t.store.SetSyncState(key, t.now()); err != nil {
		return fmt.Errorf("record Telegram disconnect notice marker: %w", err)
	}
	chatID, threadID, err := TelegramDecomposeConversationID(delivery.ConversationID)
	if err != nil {
		return err
	}
	var meta map[string]string
	var replyTo int64
	if json.Unmarshal([]byte(delivery.Event.MetaJSON), &meta) == nil {
		replyTo, _ = strconv.ParseInt(meta["message_id"], 10, 64)
	}
	t.stopTyping(delivery.ConversationID)
	for index, chunk := range telegramSplitText(notice) {
		message := telegramSendMessage{ChatID: chatID, MessageThreadID: threadID, Text: chunk}
		if index == 0 {
			message.ReplyToMessageID = replyTo
		}
		if _, err := t.client.SendMessage(ctx, message); err != nil {
			return t.outboundError("disconnect notice", err)
		}
	}
	return nil
}

func (t *TelegramConnector) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.serverMu.Lock()
	defer t.serverMu.Unlock()
	if t.server != nil {
		return errors.New("telegram connector is already started")
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(t.opts.ListenPort)))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: t}
	t.listener = listener
	t.server = server
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.log("telegram webhook server failed: %v", err)
		}
	}()
	t.log("telegram webhook listening on %s/webhook", listener.Addr())
	return nil
}

func (t *TelegramConnector) Stop(ctx context.Context) error {
	t.serverMu.Lock()
	server := t.server
	t.serverMu.Unlock()
	var err error
	if server != nil {
		err = server.Shutdown(ctx)
		t.serverMu.Lock()
		if t.server == server {
			t.server = nil
			t.listener = nil
		}
		t.serverMu.Unlock()
	}
	t.stopAllTyping()
	return err
}

func (t *TelegramConnector) Address() string {
	t.serverMu.Lock()
	defer t.serverMu.Unlock()
	if t.listener == nil {
		return ""
	}
	return t.listener.Addr().String()
}

func (t *TelegramConnector) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/health" {
		writeTelegramJSON(w, http.StatusOK, map[string]any{"ok": true, "connector": TelegramName})
		return
	}
	if request.Method != http.MethodPost || request.URL.Path != "/webhook" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	got := request.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	if !hmac.Equal([]byte(got), []byte(t.opts.WebhookSecret)) {
		t.log("telegram drop: bad webhook secret")
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, telegramMaxWebhookBytes+1))
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if len(raw) > telegramMaxWebhookBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	var update TelegramUpdate
	if err := json.Unmarshal(raw, &update); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// Telegram retries non-2xx webhook responses. Persist inline so 200 is a
	// durable receipt, never an optimistic acknowledgement.
	if _, err := t.Ingest(request.Context(), update, raw); err != nil {
		t.log("telegram ingest failed: %v", t.safeError(err))
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func (t *TelegramConnector) Ingest(ctx context.Context, update TelegramUpdate, raw []byte) (*Event, error) {
	message := update.Message
	edited := false
	if message == nil {
		message = update.EditedMessage
		edited = message != nil
	}
	if message == nil || message.From == nil {
		return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
	}
	from := message.From
	if t.isOwnMessage(*from) {
		t.log("telegram drop: bot-authored update %d", update.UpdateID)
		return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
	}
	chatID := strconv.FormatInt(message.Chat.ID, 10)
	userID := strconv.FormatInt(from.ID, 10)
	switch message.Chat.Type {
	case "private":
		if _, allowed := t.allowedUsers[userID]; !allowed {
			t.log("telegram drop: DM sender %s not in allowlist", userID)
			return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
		}
	case "group", "supergroup":
		if _, allowed := t.allowedChats[chatID]; !allowed {
			t.log("telegram drop: group %s not in allowlist", chatID)
			return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
		}
		if t.opts.GroupRequireMention && !t.isAddressed(*message) {
			t.log("telegram group %s update %d not addressed; not delivered", chatID, update.UpdateID)
			return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
		}
	default:
		t.log("telegram drop: unsupported chat type %s", message.Chat.Type)
		return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
	}

	text := message.Text
	if text == "" {
		text = message.Caption
	}
	if t.isClearCommand(text) && t.opts.ClearDisabled {
		if !t.shadow.Suppressed() && strings.TrimSpace(t.opts.ClearAck) != "" {
			if _, err := t.client.SendMessage(ctx, telegramSendMessage{ChatID: chatID, MessageThreadID: message.MessageThreadID, Text: t.opts.ClearAck}); err != nil {
				t.log("telegram clear acknowledgement failed: %v", t.safeError(err))
			}
		}
		return nil, t.store.SetSyncState(telegramWatermarkKey, update.UpdateID)
	}

	meta := map[string]string{
		"source": "telegram-channel", "chat_id": chatID, "chat_type": message.Chat.Type,
		"message_id": strconv.FormatInt(message.MessageID, 10), "user_id": userID,
		"update_id": strconv.FormatInt(update.UpdateID, 10),
	}
	if message.Chat.Title != "" {
		meta["chat_title"] = message.Chat.Title
	}
	if message.MessageThreadID != 0 {
		meta["message_thread_id"] = strconv.FormatInt(message.MessageThreadID, 10)
	}
	if message.ReplyToMessage != nil {
		meta["reply_to"] = strconv.FormatInt(message.ReplyToMessage.MessageID, 10)
	}
	if edited {
		meta["edited"] = "true"
	}
	if message.Date != 0 {
		meta["ts"] = time.Unix(message.Date, 0).UTC().Format(time.RFC3339)
	}
	if message.Chat.Type == "group" || message.Chat.Type == "supergroup" {
		stripped := t.stripLeadingMention(text)
		if stripped != text {
			meta["raw_text"] = text
			text = stripped
		}
	}
	attachment := telegramMessageAttachment(*message)
	if attachment.FileID != "" {
		meta["attachment_file_id"] = attachment.FileID
		if attachment.Name != "" {
			meta["attachment_filename"] = attachment.Name
		}
		if attachment.MIMEType != "" {
			meta["attachment_mime"] = attachment.MIMEType
		}
		if !t.shadow.Suppressed() {
			path, err := t.client.DownloadFile(ctx, attachment.FileID)
			if err != nil {
				t.log("telegram attachment download failed for update %d: %v", update.UpdateID, t.safeError(err))
			} else {
				meta["attachment_path"] = path
				if strings.HasPrefix(attachment.MIMEType, "image/") {
					meta["image_path"] = path
				}
				if strings.TrimSpace(text) == "" {
					text = "(attachment)"
				}
				text += "\n\nAttachment (local path): " + path
			}
		}
	}
	if strings.TrimSpace(text) == "" {
		text = "(empty message)"
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		raw, err = json.Marshal(update)
		if err != nil {
			return nil, err
		}
	}
	conversationID := TelegramConversationID(message.Chat.ID, message.MessageThreadID)
	user := telegramDisplayName(*from)
	now := t.now()
	// update_id is Telegram's stable identity for one inbound update; webhook
	// retries reuse it. Changing this key would either replay old updates or
	// collapse distinct edits, so the `update:` form is frozen with the ledger.
	eventKey := "update:" + strconv.FormatInt(update.UpdateID, 10)
	event, err := t.store.InsertEvent(EventInsert{
		Connector: TelegramName, EventKey: eventKey, ConversationID: conversationID,
		User: &user, Content: text, MetaJSON: string(metaJSON), RawJSON: string(raw),
	}, now)
	if err != nil {
		return nil, err
	}
	inserted := event != nil
	if event == nil {
		event, err = t.store.FindEvent(TelegramName, eventKey)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return nil, errors.New("telegram duplicate update disappeared from the ledger")
		}
	}
	// Ensure a crash between event and delivery creation repairs itself on the
	// webhook retry rather than turning the unique event row into a dead letter.
	if _, err := t.store.InsertDelivery(event.ID, t.target, now); err != nil {
		return nil, err
	}
	// Webhook delivery itself resumes through Telegram's retry queue. This
	// durable high-water mark is diagnostic and restart-verifiable; it never
	// filters lower updates, because concurrent webhook requests may finish out
	// of order and event_key dedupe is the safe replay gate.
	if err := t.store.SetSyncState(telegramWatermarkKey, update.UpdateID); err != nil {
		return nil, err
	}
	if !inserted {
		t.log("telegram duplicate update %d already queued", update.UpdateID)
		return nil, nil
	}
	t.startTyping(conversationID)
	t.log("telegram queued update %d as event %d conversation %s", update.UpdateID, event.ID, conversationID)
	return event, nil
}

func TelegramConversationID(chatID, threadID int64) string {
	conversationID := strconv.FormatInt(chatID, 10)
	if threadID != 0 {
		conversationID += ":" + strconv.FormatInt(threadID, 10)
	}
	return conversationID
}

func TelegramDecomposeConversationID(conversationID string) (chatID string, threadID int64, err error) {
	conversationID = strings.TrimSpace(conversationID)
	separator := strings.IndexByte(conversationID, ':')
	if separator < 0 {
		if conversationID == "" {
			return "", 0, errors.New("Telegram conversation_id names no chat")
		}
		return conversationID, 0, nil
	}
	chatID = conversationID[:separator]
	thread := conversationID[separator+1:]
	if chatID == "" || thread == "" {
		return "", 0, fmt.Errorf("Telegram conversation_id %q is malformed", conversationID)
	}
	threadID, parseErr := strconv.ParseInt(thread, 10, 64)
	if parseErr != nil || threadID <= 0 {
		return "", 0, fmt.Errorf("Telegram conversation_id %q has an invalid message thread", conversationID)
	}
	return chatID, threadID, nil
}

func (t *TelegramConnector) isOwnMessage(user TelegramUser) bool {
	if t.opts.BotUserID != "" && strconv.FormatInt(user.ID, 10) == t.opts.BotUserID {
		return true
	}
	return t.opts.BotUsername != "" && strings.EqualFold(user.Username, t.opts.BotUsername)
}

func (t *TelegramConnector) isAddressed(message TelegramMessage) bool {
	text := message.Text
	entities := message.Entities
	if text == "" {
		text = message.Caption
		entities = message.CaptionEntities
	}
	if t.mentionsBot(text, entities) || t.isCommandForBot(text) {
		return true
	}
	return message.ReplyToMessage != nil && strings.EqualFold(message.ReplyToMessage.FromUsername(), t.opts.BotUsername)
}

func (m TelegramMessage) FromUsername() string {
	if m.From == nil {
		return ""
	}
	return m.From.Username
}

func (t *TelegramConnector) mentionsBot(text string, entities []TelegramEntity) bool {
	if t.opts.BotUsername == "" || text == "" {
		return false
	}
	needle := "@" + strings.ToLower(t.opts.BotUsername)
	lower := strings.ToLower(text)
	for index := strings.Index(lower, needle); index >= 0; {
		end := index + len(needle)
		if (index == 0 || unicode.IsSpace(rune(lower[index-1]))) && (end == len(lower) || !telegramUsernameChar(lower[end])) {
			return true
		}
		next := strings.Index(lower[end:], needle)
		if next < 0 {
			break
		}
		index = end + next
	}
	for _, entity := range entities {
		if entity.Type == "text_mention" && entity.User != nil && strings.EqualFold(entity.User.Username, t.opts.BotUsername) {
			return true
		}
	}
	return false
}

func telegramUsernameChar(char byte) bool {
	return char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_'
}

func (t *TelegramConnector) isCommandForBot(text string) bool {
	token := strings.Fields(strings.TrimSpace(text))
	if len(token) == 0 || !strings.HasPrefix(token[0], "/") {
		return false
	}
	at := strings.IndexByte(token[0], '@')
	return at < 0 || strings.EqualFold(token[0][at+1:], t.opts.BotUsername)
}

func (t *TelegramConnector) isClearCommand(text string) bool {
	text = strings.TrimSpace(text)
	return text == "/clear" || t.opts.BotUsername != "" && strings.EqualFold(text, "/clear@"+t.opts.BotUsername)
}

func (t *TelegramConnector) stripLeadingMention(text string) string {
	if t.opts.BotUsername == "" {
		return text
	}
	trimmed := strings.TrimLeftFunc(text, unicode.IsSpace)
	mention := "@" + t.opts.BotUsername
	if len(trimmed) < len(mention) || !strings.EqualFold(trimmed[:len(mention)], mention) {
		return text
	}
	if len(trimmed) > len(mention) && telegramUsernameChar(strings.ToLower(trimmed)[len(mention)]) {
		return text
	}
	return strings.TrimLeft(trimmed[len(mention):], " \t")
}

func telegramMessageAttachment(message TelegramMessage) telegramAttachment {
	if len(message.Photo) != 0 {
		largest := message.Photo[0]
		for _, photo := range message.Photo[1:] {
			if photo.FileSize > largest.FileSize || photo.FileSize == largest.FileSize && photo.Width*photo.Height > largest.Width*largest.Height {
				largest = photo
			}
		}
		return telegramAttachment{FileID: largest.FileID, Name: largest.FileUniqueID + ".jpg", MIMEType: "image/jpeg"}
	}
	for _, media := range []*TelegramMedia{message.Document, message.Voice, message.Video, message.Audio} {
		if media == nil {
			continue
		}
		name := media.FileName
		if name == "" {
			name = media.FileUniqueID
		}
		return telegramAttachment{FileID: media.FileID, Name: name, MIMEType: media.MIMEType}
	}
	return telegramAttachment{}
}

func telegramDisplayName(user TelegramUser) string {
	if user.Username != "" {
		return user.Username
	}
	name := strings.TrimSpace(user.FirstName + " " + user.LastName)
	if name != "" {
		return name
	}
	return strconv.FormatInt(user.ID, 10)
}

func telegramIDSet(raw string) map[string]struct{} {
	result := make(map[string]struct{})
	for value := range strings.SplitSeq(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

func telegramSplitText(text string) []string {
	units := 0
	for _, char := range text {
		units += 1
		if char > 0xffff {
			units++
		}
	}
	if units <= telegramMessageUTF16Limit {
		return []string{text}
	}
	chunks := make([]string, 0, units/telegramMessageUTF16Limit+1)
	var builder strings.Builder
	currentUnits := 0
	for len(text) != 0 {
		char, size := utf8.DecodeRuneInString(text)
		charUnits := 1
		if char > 0xffff {
			charUnits = 2
		}
		if currentUnits+charUnits > telegramMessageUTF16Limit {
			chunks = append(chunks, builder.String())
			builder.Reset()
			currentUnits = 0
		}
		builder.WriteString(text[:size])
		currentUnits += charUnits
		text = text[size:]
	}
	if builder.Len() != 0 {
		chunks = append(chunks, builder.String())
	}
	return chunks
}

func (t *TelegramConnector) startTyping(conversationID string) {
	if t.shadow.Suppressed() {
		return
	}
	chatID, threadID, err := TelegramDecomposeConversationID(conversationID)
	if err != nil {
		return
	}
	t.stopTyping(conversationID)
	ctx, cancel := context.WithCancel(context.Background())
	t.typingMu.Lock()
	t.typing[conversationID] = cancel
	t.typingWG.Add(1)
	t.typingMu.Unlock()
	go func() {
		defer t.typingWG.Done()
		maximum := time.NewTimer(t.typingMaximum)
		defer maximum.Stop()
		refresh := time.NewTicker(t.typingRefresh)
		defer refresh.Stop()
		for {
			if err := t.client.SendChatAction(ctx, chatID, threadID, "typing"); err != nil && ctx.Err() == nil {
				t.log("telegram typing indicator failed: %v", t.safeError(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-maximum.C:
				t.stopTyping(conversationID)
				return
			case <-refresh.C:
			}
		}
	}()
}

func (t *TelegramConnector) stopTyping(conversationID string) {
	t.typingMu.Lock()
	cancel := t.typing[conversationID]
	delete(t.typing, conversationID)
	t.typingMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (t *TelegramConnector) stopAllTyping() {
	t.typingMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(t.typing))
	for _, cancel := range t.typing {
		cancels = append(cancels, cancel)
	}
	clear(t.typing)
	t.typingMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	t.typingWG.Wait()
}

func (t *TelegramConnector) safeError(err error) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(err.Error(), t.opts.BotToken, "[REDACTED]")
}

func (t *TelegramConnector) outboundError(action string, err error) error {
	return fmt.Errorf("telegram %s failed: %s", action, t.safeError(err))
}

func writeTelegramJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
