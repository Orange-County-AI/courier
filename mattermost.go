package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	mmDMHistoryLimit    = 20
	mmMaxAttachmentSize = int64(50 * 1024 * 1024)
	mmTypingRefresh     = 3 * time.Second
	mmTypingMaximum     = 90 * time.Second
	mmKeepalive         = 30 * time.Second
	mmWatchdog          = 60 * time.Second
	mmReconnectInitial  = 500 * time.Millisecond
	mmReconnectMaximum  = 30 * time.Second
	mmCatchUpRetry      = 30 * time.Second
)

var mmMattermostTools = []ToolDef{{
	Name: "mattermost_history",
	Description: "Read recent Mattermost messages: a whole thread (root_id) or a channel's recent posts " +
		"(channel_id). Use it when a message refers to something said earlier that you were not sent. " +
		"Returns posts oldest-first with author, timestamp and any attachments.",
	InputSchema: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"channel_id": map[string]any{"type": "string", "description": "Channel to read. From the message's conversation_id."},
			"root_id":    map[string]any{"type": "string", "description": "Thread root to read instead of a channel."},
			"limit":      map[string]any{"type": "number", "description": "Max channel posts (default 30). Ignored with root_id."},
		},
	},
}}

const mmMattermostInstructions = "Mattermost messages arrive with connector=\"mattermost\". In channels, an @mention must receive a visible " +
	"chat_reply; Courier routes it into a thread rooted at the mentioned post. Once the bot is mentioned in a thread, every later message in " +
	"that thread is delivered with trigger=\"thread\" whether or not it mentions the bot; use your judgment to chat_reply or mark_handled. " +
	"Channel conversation_id values are `<channel_id>:<root_id>` and must still be passed back unchanged. In DMs, omit reply_mode to answer " +
	"where the message arrived, or set chat_reply reply_mode to \"root\" or \"thread\" when one location better fits the exchange. " +
	"Use mattermost_history to read more context. There is no Mattermost-specific mark-handled tool: mark_handled covers it."

type mmSocket interface {
	Read(context.Context) ([]byte, error)
	Write(context.Context, []byte) error
	Close() error
}

type MMSocketFactory func(context.Context, string, string) (mmSocket, error)

type mmCoderSocket struct {
	connection *websocket.Conn
}

func (s *mmCoderSocket) Read(ctx context.Context) ([]byte, error) {
	messageType, data, err := s.connection.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, nil
	}
	return data, nil
}

func (s *mmCoderSocket) Write(ctx context.Context, data []byte) error {
	return s.connection.Write(ctx, websocket.MessageText, data)
}

func (s *mmCoderSocket) Close() error {
	return s.connection.Close(websocket.StatusNormalClosure, "courier stopping")
}

func mmOpenSocket(ctx context.Context, socketURL, token string) (mmSocket, error) {
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+token)
	connection, _, err := websocket.Dial(ctx, socketURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, err
	}
	connection.SetReadLimit(8 << 20)
	return &mmCoderSocket{connection: connection}, nil
}

type MattermostConnectorConfig struct {
	Store         *Store
	Target        string
	Options       MattermostOptions
	Shadow        ShadowMode
	Client        MattermostAPI
	SocketFactory MMSocketFactory
	Now           func() int64
	Log           func(string, ...any)

	TypingRefresh    time.Duration
	TypingMaximum    time.Duration
	Keepalive        time.Duration
	Watchdog         time.Duration
	ReconnectInitial time.Duration
	ReconnectMaximum time.Duration
	CatchUpRetry     time.Duration
}

type mmTypingState struct {
	cancel context.CancelFunc
}

type MattermostConnector struct {
	store      *Store
	target     string
	opts       MattermostOptions
	shadow     ShadowMode
	client     MattermostAPI
	openSocket MMSocketFactory
	now        func() int64
	log        func(string, ...any)

	typingRefresh    time.Duration
	typingMaximum    time.Duration
	keepalive        time.Duration
	watchdog         time.Duration
	reconnectInitial time.Duration
	reconnectMaximum time.Duration
	catchUpRetry     time.Duration

	identityMu  sync.RWMutex
	botUserID   string
	botUsername string

	cacheMu      sync.Mutex
	userNames    map[string]string
	channelCache map[string]MMChannel

	socketMu  sync.Mutex
	socket    mmSocket
	socketSeq int64

	typingMu sync.Mutex
	typing   map[string]mmTypingState

	catchUpMu      sync.Mutex
	catchUpStateMu sync.Mutex
	catchUpRunning bool
	catchUpQueued  bool

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	loopWG      sync.WaitGroup
	eventWG     sync.WaitGroup
}

func NewMattermostConnector(config MattermostConnectorConfig) (*MattermostConnector, error) {
	if config.Store == nil {
		return nil, errors.New("mattermost connector requires a store")
	}
	if strings.TrimSpace(config.Target) == "" {
		return nil, errors.New("mattermost connector requires a target")
	}
	client := config.Client
	if client == nil {
		if strings.TrimSpace(config.Options.URL) == "" || strings.TrimSpace(config.Options.BotToken) == "" {
			return nil, errors.New("mattermost connector requires MATTERMOST_URL and MATTERMOST_BOT_TOKEN")
		}
		created, err := NewMattermostClient(config.Options.URL, config.Options.BotToken, nil)
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
	openSocket := config.SocketFactory
	if openSocket == nil {
		openSocket = mmOpenSocket
	}
	attachmentDir := strings.TrimSpace(config.Options.AttachmentDir)
	if attachmentDir == "" {
		attachmentDir = "./data/attachments/mattermost"
		config.Options.AttachmentDir = attachmentDir
	}
	return &MattermostConnector{
		store: config.Store, target: strings.TrimSpace(config.Target), opts: config.Options,
		shadow: config.Shadow, client: client, openSocket: openSocket, now: now, log: log,
		typingRefresh:    mmDuration(config.TypingRefresh, mmTypingRefresh),
		typingMaximum:    mmDuration(config.TypingMaximum, mmTypingMaximum),
		keepalive:        mmDuration(config.Keepalive, mmKeepalive),
		watchdog:         mmDuration(config.Watchdog, mmWatchdog),
		reconnectInitial: mmDuration(config.ReconnectInitial, mmReconnectInitial),
		reconnectMaximum: mmDuration(config.ReconnectMaximum, mmReconnectMaximum),
		catchUpRetry:     mmDuration(config.CatchUpRetry, mmCatchUpRetry),
		botUserID:        config.Options.BotUserID, botUsername: "the bot",
		userNames: make(map[string]string), channelCache: make(map[string]MMChannel),
		typing: make(map[string]mmTypingState), socketSeq: 1000,
	}, nil
}

func mmDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func (m *MattermostConnector) Name() string { return MattermostName }

func (m *MattermostConnector) ManifestTools() []ToolDef {
	return append([]ToolDef(nil), mmMattermostTools...)
}

func (m *MattermostConnector) Instructions() string { return mmMattermostInstructions }

func MMConversationIDFor(normalized MMNormalized) string {
	if normalized.ChannelType != "D" {
		return normalized.ChannelID + ":" + normalized.RootID
	}
	if MMInThread(normalized.Post) {
		return normalized.ChannelID + ":" + normalized.RootID
	}
	return normalized.ChannelID
}

func MMDecomposeConversationID(conversationID string) (channelID, rootID string) {
	separator := strings.IndexByte(conversationID, ':')
	if separator < 0 {
		return conversationID, ""
	}
	return conversationID[:separator], conversationID[separator+1:]
}

func (m *MattermostConnector) Identity() (userID, username string) {
	m.identityMu.RLock()
	defer m.identityMu.RUnlock()
	return m.botUserID, m.botUsername
}

func (m *MattermostConnector) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.lifecycleMu.Lock()
	if m.cancel != nil {
		m.lifecycleMu.Unlock()
		return errors.New("mattermost connector is already started")
	}
	lifetime, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.lifecycleMu.Unlock()

	me, err := m.client.Me(lifetime)
	if err != nil {
		m.lifecycleMu.Lock()
		m.cancel = nil
		m.lifecycleMu.Unlock()
		cancel()
		return err
	}
	m.identityMu.Lock()
	m.botUsername = me.Username
	if m.botUserID == "" {
		m.botUserID = me.ID
	} else if m.botUserID != me.ID {
		m.log("WARN: configured bot user_id %s != token identity %s — using the configured value", m.botUserID, me.ID)
	}
	botUserID := m.botUserID
	m.identityMu.Unlock()
	m.log("Mattermost token OK — @%s (%s); filtering for bot user_id %s", me.Username, me.ID, botUserID)

	watermark, err := m.store.GetSyncState(MMWatermarkKey)
	if err != nil {
		cancel()
		m.lifecycleMu.Lock()
		m.cancel = nil
		m.lifecycleMu.Unlock()
		return err
	}
	if watermark == nil {
		if err := m.store.SetSyncState(MMWatermarkKey, m.now()); err != nil {
			cancel()
			m.lifecycleMu.Lock()
			m.cancel = nil
			m.lifecycleMu.Unlock()
			return err
		}
		m.log("Mattermost first start — ingestion watermark initialized to now (no historical replay)")
	}
	m.loopWG.Add(1)
	go func() {
		defer m.loopWG.Done()
		m.connectLoop(lifetime)
	}()
	return nil
}

func (m *MattermostConnector) Stop(ctx context.Context) error {
	m.lifecycleMu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.closeActiveSocket()
	m.stopAllTyping()
	loopsDone := make(chan struct{})
	go func() {
		m.loopWG.Wait()
		close(loopsDone)
	}()
	select {
	case <-loopsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	eventsDone := make(chan struct{})
	go func() {
		m.eventWG.Wait()
		close(eventsDone)
	}()
	select {
	case <-eventsDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MattermostConnector) connectLoop(ctx context.Context) {
	delay := m.reconnectInitial
	for ctx.Err() == nil {
		socket, err := m.openSocket(ctx, m.client.WebSocketURL(), m.opts.BotToken)
		if err != nil {
			m.log("Mattermost websocket dial failed: %v — reconnecting in %s", err, delay)
			if !mmWait(ctx, delay) {
				return
			}
			delay = mmNextDelay(delay, m.reconnectMaximum)
			continue
		}
		authenticated, err := m.runSocket(ctx, socket)
		_ = socket.Close()
		m.clearActiveSocket(socket)
		if ctx.Err() != nil {
			return
		}
		if authenticated {
			delay = m.reconnectInitial
		}
		m.log("Mattermost websocket down: %v — reconnecting in %s", err, delay)
		if !mmWait(ctx, delay) {
			return
		}
		delay = mmNextDelay(delay, m.reconnectMaximum)
	}
}

type mmSocketRead struct {
	data []byte
	err  error
}

func (m *MattermostConnector) runSocket(ctx context.Context, socket mmSocket) (bool, error) {
	m.setActiveSocket(socket)
	if err := m.writeSocket(ctx, socket, map[string]any{
		"action": "authentication_challenge",
		"data":   map[string]any{"token": m.opts.BotToken},
	}); err != nil {
		return false, err
	}
	readChannel := make(chan mmSocketRead, 1)
	go func() {
		for {
			data, err := socket.Read(ctx)
			select {
			case readChannel <- mmSocketRead{data: data, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	keepalive := time.NewTicker(m.keepalive)
	defer keepalive.Stop()
	watchdogInterval := m.watchdog / 2
	if watchdogInterval <= 0 {
		watchdogInterval = m.watchdog
	}
	watchdog := time.NewTicker(watchdogInterval)
	defer watchdog.Stop()
	lastActivity := m.now()
	authenticated := false
	for {
		select {
		case <-ctx.Done():
			return authenticated, ctx.Err()
		case <-keepalive.C:
			if err := m.writeSocket(ctx, socket, map[string]any{"action": "get_statuses"}); err != nil {
				return authenticated, err
			}
		case <-watchdog.C:
			if m.now()-lastActivity > m.watchdog.Milliseconds() {
				return authenticated, errors.New("watchdog: no websocket activity")
			}
		case read := <-readChannel:
			if read.err != nil {
				return authenticated, read.err
			}
			if len(read.data) == 0 {
				continue
			}
			lastActivity = m.now()
			var event MMEvent
			if err := json.Unmarshal(read.data, &event); err != nil {
				continue
			}
			if event.Event == "hello" {
				authenticated = true
				m.log("Mattermost websocket authenticated")
				m.scheduleCatchUp(ctx, "ws connected")
				continue
			}
			raw := string(read.data)
			m.eventWG.Add(1)
			go func(event MMEvent) {
				defer m.eventWG.Done()
				if err := m.HandleEvent(ctx, event, raw, false); err != nil {
					m.log("Mattermost %s ingest failed: %v", event.Event, err)
					return
				}
				if createdAt := MMPostedCreateAt(event); createdAt != nil {
					if err := m.store.SetSyncState(MMWatermarkKey, *createdAt); err != nil {
						m.log("Mattermost watermark update failed: %v", err)
					}
				}
			}(event)
		}
	}
}

func (m *MattermostConnector) scheduleCatchUp(ctx context.Context, reason string) {
	m.catchUpStateMu.Lock()
	if m.catchUpRunning {
		m.catchUpQueued = true
		m.catchUpStateMu.Unlock()
		return
	}
	m.catchUpRunning = true
	m.catchUpStateMu.Unlock()

	m.eventWG.Add(1)
	go func() {
		defer m.eventWG.Done()
		for {
			if err := m.RunCatchUp(ctx, reason); err != nil {
				if ctx.Err() != nil {
					m.finishCatchUp()
					return
				}
				m.log("Mattermost catch-up failed: %v — retrying in %s", err, m.catchUpRetry)
				if !mmWait(ctx, m.catchUpRetry) {
					m.finishCatchUp()
					return
				}
				reason = "retry"
				continue
			}

			m.catchUpStateMu.Lock()
			if m.catchUpQueued {
				m.catchUpQueued = false
				m.catchUpStateMu.Unlock()
				reason = "queued"
				continue
			}
			m.catchUpRunning = false
			m.catchUpStateMu.Unlock()
			return
		}
	}()
}

func (m *MattermostConnector) finishCatchUp() {
	m.catchUpStateMu.Lock()
	m.catchUpRunning = false
	m.catchUpQueued = false
	m.catchUpStateMu.Unlock()
}

func (m *MattermostConnector) setActiveSocket(socket mmSocket) {
	m.socketMu.Lock()
	m.socket = socket
	m.socketMu.Unlock()
}

func (m *MattermostConnector) clearActiveSocket(socket mmSocket) {
	m.socketMu.Lock()
	if m.socket == socket {
		m.socket = nil
	}
	m.socketMu.Unlock()
}

func (m *MattermostConnector) closeActiveSocket() {
	m.socketMu.Lock()
	socket := m.socket
	m.socket = nil
	m.socketMu.Unlock()
	if socket != nil {
		_ = socket.Close()
	}
}

func (m *MattermostConnector) writeSocket(ctx context.Context, socket mmSocket, payload map[string]any) error {
	m.socketMu.Lock()
	defer m.socketMu.Unlock()
	m.socketSeq++
	payload["seq"] = m.socketSeq
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return socket.Write(ctx, encoded)
}

func (m *MattermostConnector) sendTyping(channelID, rootID string) bool {
	if m.shadow.Suppressed() {
		return false
	}
	m.socketMu.Lock()
	defer m.socketMu.Unlock()
	if m.socket == nil {
		return false
	}
	m.socketSeq++
	encoded, err := json.Marshal(map[string]any{
		"seq": m.socketSeq, "action": "user_typing",
		"data": map[string]any{"channel_id": channelID, "parent_id": rootID},
	})
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.socket.Write(ctx, encoded) == nil
}

func (m *MattermostConnector) startTyping(channelID, rootID string) {
	if m.shadow.Suppressed() {
		return
	}
	m.stopTyping(channelID)
	m.sendTyping(channelID, rootID)
	ctx, cancel := context.WithCancel(context.Background())
	m.typingMu.Lock()
	m.typing[channelID] = mmTypingState{cancel: cancel}
	m.typingMu.Unlock()
	go func() {
		refresh := time.NewTicker(m.typingRefresh)
		defer refresh.Stop()
		maximum := time.NewTimer(m.typingMaximum)
		defer maximum.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-refresh.C:
				m.sendTyping(channelID, rootID)
			case <-maximum.C:
				m.stopTyping(channelID)
				return
			}
		}
	}()
}

func (m *MattermostConnector) stopTyping(channelID string) {
	m.typingMu.Lock()
	state, found := m.typing[channelID]
	if found {
		delete(m.typing, channelID)
	}
	m.typingMu.Unlock()
	if found {
		state.cancel()
	}
}

func (m *MattermostConnector) stopAllTyping() {
	m.typingMu.Lock()
	states := make([]mmTypingState, 0, len(m.typing))
	for _, state := range m.typing {
		states = append(states, state)
	}
	clear(m.typing)
	m.typingMu.Unlock()
	for _, state := range states {
		state.cancel()
	}
}

func (m *MattermostConnector) HandleEvent(ctx context.Context, event MMEvent, raw string, replay bool) error {
	switch event.Event {
	case "posted":
		return m.ingestPost(ctx, event, raw, replay)
	case "post_edited":
		return m.ingestEdit(ctx, event, raw)
	case "post_deleted":
		return m.ingestDelete(ctx, event, raw)
	default:
		return nil
	}
}

func (m *MattermostConnector) ingestPost(ctx context.Context, event MMEvent, raw string, replay bool) error {
	botUserID, botUsername := m.Identity()
	parsed := ParseMMPost(event, botUserID)
	if parsed == nil {
		return nil
	}
	insideThread := MMInThread(parsed.Post)
	direct := parsed.IsDM || parsed.Mentioned
	if direct {
		parent := ""
		if !parsed.IsDM {
			parent = parsed.RootID
		}
		m.startTyping(parsed.Post.ChannelID, parent)
	}
	prior, label := m.fetchContext(ctx, parsed.Post, parsed.IsDM)
	participation := make([]MMPriorPost, len(prior))
	for i, post := range prior {
		participation[i] = MMPriorPost{IsBot: post.IsBot, Message: post.Message}
	}
	threadFollowed := false
	if !parsed.IsDM && parsed.Mentioned {
		if err := m.store.FollowMattermostThread(parsed.Post.ChannelID, parsed.RootID, m.now()); err != nil {
			m.stopTyping(parsed.Post.ChannelID)
			return err
		}
		threadFollowed = true
	} else if !parsed.IsDM && insideThread {
		var err error
		threadFollowed, err = m.store.IsMattermostThreadFollowed(parsed.Post.ChannelID, parsed.RootID)
		if err != nil {
			return err
		}
		if !threadFollowed && MMBotParticipates(participation, botUsername) {
			if err := m.store.FollowMattermostThread(parsed.Post.ChannelID, parsed.RootID, m.now()); err != nil {
				return err
			}
			threadFollowed = true
		}
	}
	normalized := NormalizeMMPost(event, botUserID, threadFollowed)
	if normalized == nil {
		if direct {
			m.stopTyping(parsed.Post.ChannelID)
		}
		return nil
	}
	if replay {
		normalized.Meta["replayed"] = "true"
	}
	if !direct {
		m.startTyping(normalized.ChannelID, MMTypingParentID(*normalized))
	}
	content, err := m.decorate(ctx, normalized, prior, label)
	if err != nil {
		return err
	}
	normalized.Content = content
	queued, err := m.queue(*normalized, normalized.PostID, raw, "post")
	if err != nil || !queued {
		return err
	}
	return m.store.RecordPost(PostInput{
		PostID: normalized.PostID, ChannelID: normalized.ChannelID,
		Message: normalized.Post.Message, EditAt: normalized.Post.EditAt,
	}, m.now())
}

func (m *MattermostConnector) ingestEdit(ctx context.Context, event MMEvent, raw string) error {
	botUserID, botUsername := m.Identity()
	post := ParseMMPostEvent(event, MMPostEdited, botUserID)
	if post == nil {
		return nil
	}
	previous, err := m.store.GetPostState(post.ID)
	if err != nil {
		return err
	}
	var knownEditAt *int64
	if previous != nil {
		value := previous.EditAt
		knownEditAt = &value
	}
	if !MMIsGenuineEdit(*post, knownEditAt) {
		return nil
	}
	channel, err := m.resolveChannel(ctx, post.ChannelID)
	if err != nil {
		return err
	}
	isDM := channel.Type == "D"
	insideThread := MMInThread(*post)
	prior, label := m.fetchContext(ctx, *post, isDM)
	trigger := MMTriggerEdit
	if previous == nil {
		participation := make([]MMPriorPost, len(prior))
		for i, item := range prior {
			participation[i] = MMPriorPost{IsBot: item.IsBot, Message: item.Message}
		}
		switch {
		case isDM:
			trigger = MMTriggerDM
		case MMMentions(post.Message, botUsername):
			trigger = MMTriggerMention
			rootID := post.RootID
			if rootID == "" {
				rootID = post.ID
			}
			if err := m.store.FollowMattermostThread(post.ChannelID, rootID, m.now()); err != nil {
				return err
			}
		case insideThread:
			followed, err := m.store.IsMattermostThreadFollowed(post.ChannelID, post.RootID)
			if err != nil {
				return err
			}
			if !followed && MMBotParticipates(participation, botUsername) {
				if err := m.store.FollowMattermostThread(post.ChannelID, post.RootID, m.now()); err != nil {
					return err
				}
				followed = true
			}
			if !followed {
				return nil
			}
			trigger = MMTriggerThread
		default:
			return nil
		}
	}
	parent := ""
	if !isDM && insideThread {
		parent = post.RootID
	}
	m.startTyping(post.ChannelID, parent)
	names := m.resolveNames(ctx, []string{post.UserID})
	var previousMessage *string
	if previous != nil {
		value := previous.Message
		previousMessage = &value
	}
	normalized := NormalizeMMEdit(*post, MMMutationContext{
		ChannelType: channel.Type, ChannelName: mmChannelName(channel), Team: channel.TeamID,
		Sender: names[post.UserID], PreviousMessage: previousMessage, Trigger: trigger,
	})
	content, err := m.decorate(ctx, &normalized, prior, label)
	if err != nil {
		return err
	}
	normalized.Content = content
	if _, err := m.queue(normalized, MMEditEventKey(*post), raw, "edit"); err != nil {
		return err
	}
	return m.store.RecordPost(PostInput{
		PostID: post.ID, ChannelID: post.ChannelID, Message: post.Message, EditAt: post.EditAt,
	}, m.now())
}

func (m *MattermostConnector) ingestDelete(ctx context.Context, event MMEvent, raw string) error {
	botUserID, _ := m.Identity()
	post := ParseMMPostEvent(event, MMPostDeleted, botUserID)
	if post == nil {
		return nil
	}
	previous, err := m.store.GetPostState(post.ID)
	if err != nil || previous == nil || previous.DeleteAt > 0 {
		return err
	}
	channel, err := m.resolveChannel(ctx, post.ChannelID)
	if err != nil {
		return err
	}
	parent := ""
	if channel.Type != "D" && MMInThread(*post) {
		parent = post.RootID
	}
	m.startTyping(post.ChannelID, parent)
	names := m.resolveNames(ctx, []string{post.UserID})
	previousMessage := previous.Message
	normalized := NormalizeMMDelete(*post, MMMutationContext{
		ChannelType: channel.Type, ChannelName: mmChannelName(channel), Team: channel.TeamID,
		Sender: names[post.UserID], PreviousMessage: &previousMessage, Trigger: MMTriggerEdit,
	})
	if _, err := m.queue(normalized, MMDeleteEventKey(*post), raw, "delete"); err != nil {
		return err
	}
	deleteAt := post.DeleteAt
	if deleteAt == 0 {
		deleteAt = m.now()
	}
	_, err = m.store.MarkPostDeleted(post.ID, deleteAt, m.now())
	return err
}

func (m *MattermostConnector) queue(normalized MMNormalized, eventKey, raw, eventType string) (bool, error) {
	conversationID := MMConversationIDFor(normalized)
	metaJSON, err := json.Marshal(normalized.Meta)
	if err != nil {
		return false, err
	}
	var user *string
	if normalized.User != "" {
		value := normalized.User
		user = &value
	}
	event, err := m.store.InsertEvent(EventInsert{
		Connector: MattermostName, EventKey: eventKey, ConversationID: conversationID,
		User: user, Content: normalized.Content, MetaJSON: string(metaJSON), RawJSON: raw,
	}, m.now())
	if err != nil {
		return false, err
	}
	if event == nil {
		m.log("Mattermost duplicate %s %s — already queued", eventType, eventKey)
		m.stopTyping(normalized.ChannelID)
		return false, nil
	}
	if _, err := m.store.InsertDelivery(event.ID, m.target, m.now()); err != nil {
		return false, err
	}
	m.log("Mattermost queued %s %s as event %d conversation %s", eventType, eventKey, event.ID, conversationID)
	return true, nil
}

func (m *MattermostConnector) fetchContext(ctx context.Context, post MMPost, isDM bool) ([]MMHistoryItem, string) {
	if MMInThread(post) {
		items, err := m.historyItems(ctx, "", post.RootID, 0)
		if err != nil {
			m.log("Mattermost thread context failed for %s: %v", post.ID, err)
			return nil, ""
		}
		return mmWithoutPost(items, post.ID), "earlier in this thread"
	}
	if isDM {
		items, err := m.historyItems(ctx, post.ChannelID, "", mmDMHistoryLimit)
		if err != nil {
			m.log("Mattermost DM context failed for %s: %v", post.ID, err)
			return nil, ""
		}
		return mmWithoutPost(items, post.ID), "earlier in this DM"
	}
	return nil, ""
}

func (m *MattermostConnector) historyItems(ctx context.Context, channelID, rootID string, limit int) ([]MMHistoryItem, error) {
	var list MMPostList
	var err error
	if rootID != "" {
		list, err = m.client.GetThread(ctx, rootID)
	} else {
		list, err = m.client.GetChannelPosts(ctx, channelID, limit)
	}
	if err != nil {
		return nil, err
	}
	posts := make([]MMPost, 0, len(list.Order))
	for _, id := range list.Order {
		if post, ok := list.Posts[id]; ok {
			posts = append(posts, post)
		}
	}
	mmSortPosts(posts)
	ids := make([]string, len(posts))
	for i, post := range posts {
		ids[i] = post.UserID
	}
	names := m.resolveNames(ctx, ids)
	botUserID, _ := m.Identity()
	items := make([]MMHistoryItem, len(posts))
	for i, post := range posts {
		items[i] = MMHistoryItem{
			PostID: post.ID, User: names[post.UserID], IsBot: post.UserID == botUserID,
			At: mmISOTime(post.CreateAt), Message: post.Message, Files: ExtractMMFiles(post),
		}
	}
	return items, nil
}

func (m *MattermostConnector) resolveNames(ctx context.Context, ids []string) map[string]string {
	unique := make(map[string]bool)
	m.cacheMu.Lock()
	missing := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, found := m.userNames[id]; !found && !unique[id] {
			unique[id] = true
			missing = append(missing, id)
		}
	}
	m.cacheMu.Unlock()
	if len(missing) > 0 {
		users, err := m.client.UsersByIDs(ctx, missing)
		if err != nil {
			m.log("Mattermost usersByIds failed: %v", err)
		} else {
			m.cacheMu.Lock()
			for _, user := range users {
				m.userNames[user.ID] = user.Username
			}
			m.cacheMu.Unlock()
		}
	}
	result := make(map[string]string, len(ids))
	m.cacheMu.Lock()
	for _, id := range ids {
		name := m.userNames[id]
		if name == "" {
			name = id
		}
		result[id] = name
	}
	m.cacheMu.Unlock()
	return result
}

func (m *MattermostConnector) resolveChannel(ctx context.Context, channelID string) (MMChannel, error) {
	m.cacheMu.Lock()
	channel, found := m.channelCache[channelID]
	m.cacheMu.Unlock()
	if found {
		return channel, nil
	}
	channel, err := m.client.GetChannel(ctx, channelID)
	if err != nil {
		return MMChannel{}, err
	}
	m.cacheMu.Lock()
	m.channelCache[channelID] = channel
	m.cacheMu.Unlock()
	return channel, nil
}

func (m *MattermostConnector) decorate(ctx context.Context, normalized *MMNormalized, prior []MMHistoryItem, label string) (string, error) {
	content := normalized.Content
	if len(normalized.Files) > 0 {
		saved := m.downloadAttachments(ctx, normalized.PostID, normalized.Files)
		if len(saved) > 0 {
			content += "\n\n" + MMFormatAttachmentBlock(saved)
			for _, attachment := range saved {
				if attachment.IsImage && attachment.Path != "" {
					normalized.Meta["image_path"] = attachment.Path
					break
				}
			}
		}
	}
	if len(prior) > 0 {
		attachments := make(map[string][]MMSavedAttachment)
		for _, item := range prior {
			if len(item.Files) > 0 {
				attachments[item.PostID] = m.downloadAttachments(ctx, item.PostID, item.Files)
			}
		}
		_, botUsername := m.Identity()
		content += "\n\n" + MMFormatContextBlock(prior, label, botUsername, attachments)
		normalized.Meta["context_posts"] = fmt.Sprint(len(prior))
	}
	return content, nil
}

func (m *MattermostConnector) downloadAttachments(ctx context.Context, postID string, files []MMNormalizedFile) []MMSavedAttachment {
	directory := filepath.Join(m.opts.AttachmentDir, postID)
	out := make([]MMSavedAttachment, 0, len(files))
	for _, file := range files {
		name, mimeType, size := file.Name, file.MIME, file.Size
		if name == "" || mimeType == "" || size == 0 {
			info, err := m.client.GetFileInfo(ctx, file.ID)
			if err != nil {
				m.log("Mattermost attachment info failed for %s: %v", file.ID, err)
				out = append(out, MMSavedAttachment{Name: mmValue(name, file.ID), MIME: mmValue(mimeType, "unknown"), Note: "download failed"})
				continue
			}
			if name == "" {
				name = info.Name
			}
			if mimeType == "" {
				mimeType = info.MIMEType
			}
			if size == 0 {
				size = info.Size
			}
		}
		name = mmValue(name, file.ID)
		mimeType = mmValue(mimeType, "application/octet-stream")
		if size > mmMaxAttachmentSize {
			out = append(out, MMSavedAttachment{Name: name, MIME: mimeType, Note: fmt.Sprintf("too large to auto-download (%d bytes)", size)})
			continue
		}
		content, err := m.client.GetFileBytes(ctx, file.ID, mmMaxAttachmentSize)
		if err != nil {
			note := "download failed"
			if errors.Is(err, ErrMMAttachmentTooLarge) {
				note = "too large to auto-download"
			}
			out = append(out, MMSavedAttachment{Name: name, MIME: mimeType, Note: note})
			continue
		}
		if err := os.MkdirAll(directory, 0o755); err != nil {
			out = append(out, MMSavedAttachment{Name: name, MIME: mimeType, Note: "download failed"})
			continue
		}
		path := filepath.Join(directory, file.ID+"-"+mmSafeName(name))
		if err := os.WriteFile(path, content, 0o644); err != nil {
			out = append(out, MMSavedAttachment{Name: name, MIME: mimeType, Note: "download failed"})
			continue
		}
		out = append(out, MMSavedAttachment{Name: name, Path: path, MIME: mimeType, IsImage: strings.HasPrefix(mimeType, "image/")})
	}
	return out
}

func (m *MattermostConnector) RunCatchUp(ctx context.Context, reason string) error {
	m.catchUpMu.Lock()
	defer m.catchUpMu.Unlock()
	watermark, err := m.store.GetSyncState(MMWatermarkKey)
	if err != nil {
		return err
	}
	if watermark == nil {
		return m.store.SetSyncState(MMWatermarkKey, m.now())
	}
	channels, err := m.watchedChannels(ctx)
	if err != nil {
		return err
	}
	type mmMissedPost struct {
		post    MMPost
		channel MMChannel
	}
	type mmMutation struct {
		post MMPost
		kind MMPostEventKind
	}
	var missed []mmMissedPost
	var mutations []mmMutation
	botUserID, botUsername := m.Identity()
	for _, channel := range channels {
		list, err := m.client.GetChannelPostsSince(ctx, channel.ID, *watermark)
		if err != nil {
			m.log("Mattermost catch-up fetch failed for channel %s: %v", channel.ID, err)
			continue
		}
		for _, id := range list.Order {
			post, found := list.Posts[id]
			if !found || post.UserID == botUserID {
				continue
			}
			switch {
			case post.DeleteAt > 0:
				mutations = append(mutations, mmMutation{post: post, kind: MMPostDeleted})
			case post.CreateAt > *watermark:
				missed = append(missed, mmMissedPost{post: post, channel: channel})
			case post.EditAt > 0:
				mutations = append(mutations, mmMutation{post: post, kind: MMPostEdited})
			}
		}
	}
	sort.Slice(missed, func(i, j int) bool { return missed[i].post.CreateAt < missed[j].post.CreateAt })
	ids := make([]string, len(missed))
	for i, item := range missed {
		ids[i] = item.post.UserID
	}
	names := m.resolveNames(ctx, ids)
	for _, item := range missed {
		event := MMReplayEvent(item.post, MMReplayChannel{
			ID: item.channel.ID, Type: item.channel.Type, Name: item.channel.Name,
			DisplayName: item.channel.DisplayName, TeamID: item.channel.TeamID,
		}, names[item.post.UserID], botUserID, botUsername)
		raw := mmMustJSON(map[string]any{"event": event.Event, "data": event.Data, "replayed": true})
		if err := m.ingestPost(ctx, event, raw, true); err != nil {
			return err
		}
		if err := m.store.SetSyncState(MMWatermarkKey, item.post.CreateAt); err != nil {
			return err
		}
	}
	for _, mutation := range mutations {
		event := MMMutationEvent(mutation.post, mutation.kind)
		raw := mmMustJSON(map[string]any{"event": event.Event, "data": event.Data, "replayed": true})
		if mutation.kind == MMPostEdited {
			if err := m.ingestEdit(ctx, event, raw); err != nil {
				return err
			}
		} else if err := m.ingestDelete(ctx, event, raw); err != nil {
			return err
		}
	}
	m.log("Mattermost catch-up (%s) complete", reason)
	return nil
}

func (m *MattermostConnector) watchedChannels(ctx context.Context) ([]MMChannel, error) {
	channels, err := m.client.GetAllMyChannels(ctx)
	if err == nil {
		return channels, nil
	}
	teams, teamsErr := m.client.GetMyTeams(ctx)
	if teamsErr != nil {
		return nil, teamsErr
	}
	byID := make(map[string]MMChannel)
	for _, team := range teams {
		teamChannels, channelErr := m.client.GetMyTeamChannels(ctx, team.ID)
		if channelErr != nil {
			return nil, channelErr
		}
		for _, channel := range teamChannels {
			byID[channel.ID] = channel
		}
	}
	channels = make([]MMChannel, 0, len(byID))
	for _, channel := range byID {
		channels = append(channels, channel)
	}
	return channels, nil
}

func (m *MattermostConnector) RouteReply(delivery DeliveryContext, mode string) (string, error) {
	var meta map[string]string
	if err := json.Unmarshal([]byte(delivery.Event.MetaJSON), &meta); err != nil {
		return "", fmt.Errorf("Mattermost event %d has invalid meta_json: %w", delivery.Event.ID, err)
	}
	if meta["channel_type"] != "D" {
		return "", errors.New("reply_mode is only available for Mattermost DMs")
	}
	channelID := meta["channel_id"]
	if channelID == "" {
		return "", errors.New("Mattermost DM metadata has no channel_id")
	}
	switch mode {
	case "root":
		return channelID, nil
	case "thread":
		rootID := meta["root_id"]
		if rootID == "" {
			rootID = meta["post_id"]
		}
		if rootID == "" {
			return "", errors.New("Mattermost DM metadata has no post or thread root")
		}
		return channelID + ":" + rootID, nil
	default:
		return "", fmt.Errorf("unsupported Mattermost reply_mode %q", mode)
	}
}

func (m *MattermostConnector) PostReply(ctx context.Context, delivery DeliveryContext, message string) error {
	if err := m.shadow.Refuse("posting to Mattermost"); err != nil {
		return err
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return errors.New("refusing to post an empty message to Mattermost")
	}
	channelID, rootID := MMDecomposeConversationID(delivery.ConversationID)
	if channelID == "" {
		return fmt.Errorf("conversation_id %q has no channel", delivery.ConversationID)
	}
	created, err := m.client.CreatePost(ctx, MMCreatePost{ChannelID: channelID, Message: message, RootID: rootID})
	if err != nil {
		return err
	}
	m.stopTyping(channelID)
	postTime := m.now()
	confirmed, err := m.store.MarkConversationHandled(MattermostName, delivery.ConversationID, postTime, postTime)
	if err != nil {
		return err
	}
	m.log("Mattermost posted reply %s to %s; auto-handled %d event(s)", created.ID, delivery.ConversationID, confirmed)
	return nil
}

func (m *MattermostConnector) CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
	if name != "mattermost_history" {
		return ToolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
	channelID, _ := args["channel_id"].(string)
	rootID, _ := args["root_id"].(string)
	if strings.Contains(channelID, ":") {
		var decomposedRoot string
		channelID, decomposedRoot = MMDecomposeConversationID(channelID)
		if rootID == "" {
			rootID = decomposedRoot
		}
	}
	if channelID == "" && rootID == "" {
		return ToolResult{Status: 400, Text: "mattermost_history requires channel_id or root_id", IsError: true}, nil
	}
	limit := 30
	if value, ok := args["limit"].(float64); ok && value > 0 {
		limit = int(value)
	}
	items, err := m.historyItems(ctx, channelID, rootID, limit)
	if err != nil {
		return ToolResult{}, err
	}
	if len(items) == 0 {
		return ToolResult{Text: "(no messages found)"}, nil
	}
	encoded, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{Text: string(encoded)}, nil
}

func mmChannelName(channel MMChannel) string {
	if channel.Name != "" {
		return channel.Name
	}
	return channel.DisplayName
}

func mmWithoutPost(items []MMHistoryItem, postID string) []MMHistoryItem {
	out := make([]MMHistoryItem, 0, len(items))
	for _, item := range items {
		if item.PostID != postID {
			out = append(out, item)
		}
	}
	return out
}

func mmSafeName(name string) string {
	var builder strings.Builder
	for _, char := range name {
		if char == '.' || char == '-' || char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('_')
		}
	}
	result := builder.String()
	if len(result) > 120 {
		result = result[len(result)-120:]
	}
	if result == "" {
		return "file"
	}
	return result
}

func mmValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func mmWait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func mmNextDelay(delay, maximum time.Duration) time.Duration {
	if delay >= maximum-delay {
		return maximum
	}
	return delay * 2
}
