package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	GmailName                = "gmail"
	gmailMaxAttachmentBytes  = 50 * 1024 * 1024
	gmailDefaultPollInterval = 20 * time.Second
	gmailMaxPollBackoff      = 5 * time.Minute
)

var gmailTools = []ToolDef{
	{
		Name: "gmail_reply",
		Description: "Reply inside an existing email thread. Use this for a follow-up that is not the answer to the " +
			"message you were just handed — the ordinary answer is chat_reply, which already sends into the right thread " +
			"from the right account. Threading headers and the subject are derived from the thread.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"account":   map[string]any{"type": "string", "description": "The watched address to send AS (the event's meta.account)."},
				"thread_id": map[string]any{"type": "string", "description": "The thread to reply in — the message's conversation_id."},
				"to":        map[string]any{"type": "string", "description": "Recipient address(es), comma separated."},
				"body":      map[string]any{"type": "string", "description": "The plain-text message body."},
				"subject":   map[string]any{"type": "string", "description": "Optional; defaults to Re: <the thread's subject>."},
				"cc":        map[string]any{"type": "string", "description": "Optional Cc address(es)."},
			},
			"required": []string{"account", "thread_id", "to", "body"},
		},
	},
	{
		Name: "gmail_send",
		Description: "Send a NEW email, starting its own thread. For answering something you were sent, use chat_reply " +
			"(or gmail_reply) instead — a new thread breaks the conversation the human is reading.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"account": map[string]any{"type": "string", "description": "The watched address to send AS."},
				"to":      map[string]any{"type": "string", "description": "Recipient address(es), comma separated."},
				"subject": map[string]any{"type": "string", "description": "The subject line."},
				"body":    map[string]any{"type": "string", "description": "The plain-text message body."},
				"cc":      map[string]any{"type": "string", "description": "Optional Cc address(es)."},
			},
			"required": []string{"account", "to", "subject", "body"},
		},
	},
}

const gmailInstructions = "Email arrives with connector=\"gmail\". Its conversation_id is the Gmail thread id; chat_reply sends your " +
	"answer into that thread, from the account that received it, addressed to the sender — you do not have to " +
	"assemble any of that. gmail_reply and gmail_send are for mail that is NOT the answer to a message you " +
	"were handed (a follow-up, or a new thread), and both need an explicit account."

type GmailAPI interface {
	GetProfile(context.Context) (GmailProfile, error)
	HistoryList(context.Context, string, string) (GmailHistoryPage, error)
	GetMessage(context.Context, string, string) (GmailMessage, error)
	GetAttachment(context.Context, string, string) (int64, string, error)
	GetThreadMetadata(context.Context, string) (GmailThreadMetadata, error)
	Send(context.Context, string, string) (GmailSentMessage, error)
}

type gmailWatched struct {
	config GmailAccountConfig
	client GmailAPI

	pollMu sync.Mutex
	sentMu sync.Mutex
	sentID map[string]struct{}
}

type GmailConnectorConfig struct {
	Store         *Store
	Target        string
	Accounts      []GmailAccountConfig
	AttachmentDir string
	PollInterval  time.Duration
	Shadow        ShadowMode
	ClientFactory func(GmailAccountConfig) (GmailAPI, error)
	Now           func() int64
	Sleep         func(context.Context, time.Duration) error
	Jitter        func(time.Duration) time.Duration
	Log           func(string, ...any)
}

type GmailConnector struct {
	store         *Store
	target        string
	attachmentDir string
	pollInterval  time.Duration
	shadow        ShadowMode
	now           func() int64
	sleep         func(context.Context, time.Duration) error
	jitter        func(time.Duration) time.Duration
	log           func(string, ...any)
	watched       map[string]*gmailWatched
	accountOrder  []string

	lifecycleMu sync.Mutex
	started     bool
	cancel      context.CancelFunc
	loops       sync.WaitGroup
}

func LoadGmailAccounts(options GmailOptions) ([]GmailAccountConfig, bool, error) {
	if !options.Enabled {
		return nil, false, nil
	}
	source := strings.TrimSpace(options.AccountsJSON)
	if source == "" && strings.TrimSpace(options.AccountsFile) != "" {
		contents, err := os.ReadFile(options.AccountsFile)
		if err != nil {
			return nil, true, fmt.Errorf("read GMAIL_ACCOUNTS_FILE: %w", err)
		}
		source = string(contents)
	}
	if source == "" {
		return nil, true, fmt.Errorf("GMAIL_ACCOUNTS_JSON or GMAIL_ACCOUNTS_FILE is required when Gmail is enabled")
	}
	accounts, err := ParseGmailAccounts(source)
	if err != nil {
		return nil, true, err
	}
	return accounts, true, nil
}

func NewGmailConnectorFromOptions(store *Store, target string, options GmailOptions, shadow ShadowMode) (*GmailConnector, error) {
	accounts, active, err := LoadGmailAccounts(options)
	if err != nil || !active {
		return nil, err
	}
	return NewGmailConnector(GmailConnectorConfig{
		Store: store, Target: target, Accounts: accounts, AttachmentDir: options.AttachmentDir,
		PollInterval: options.PollInterval, Shadow: shadow,
	})
}

func NewGmailConnector(config GmailConnectorConfig) (*GmailConnector, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("gmail connector requires a store")
	}
	if strings.TrimSpace(config.Target) == "" {
		return nil, fmt.Errorf("gmail connector requires a target")
	}
	if len(config.Accounts) == 0 {
		return nil, fmt.Errorf("gmail connector requires at least one account")
	}
	attachmentDir := strings.TrimSpace(config.AttachmentDir)
	if attachmentDir == "" {
		attachmentDir = "./data/attachments/gmail"
	}
	pollInterval := config.PollInterval
	if pollInterval == 0 {
		pollInterval = gmailDefaultPollInterval
	}
	now := config.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	sleep := config.Sleep
	if sleep == nil {
		sleep = gmailSleep
	}
	jitter := config.Jitter
	if jitter == nil {
		jitter = func(interval time.Duration) time.Duration {
			return gmailJitter(interval, rand.Float64())
		}
	}
	log := config.Log
	if log == nil {
		log = func(string, ...any) {}
	}
	factory := config.ClientFactory
	if factory == nil {
		factory = func(account GmailAccountConfig) (GmailAPI, error) {
			return NewGmailClient(GmailClientConfig{
				Email: account.Email, Tokens: NewGmailTokenProvider(account.TokenCommand, account.Email),
			})
		}
	}
	connector := &GmailConnector{
		store: config.Store, target: strings.TrimSpace(config.Target), attachmentDir: attachmentDir,
		pollInterval: pollInterval, shadow: config.Shadow, now: now, sleep: sleep,
		jitter: jitter, log: log, watched: make(map[string]*gmailWatched, len(config.Accounts)),
		accountOrder: make([]string, 0, len(config.Accounts)),
	}
	for _, account := range config.Accounts {
		email := strings.ToLower(strings.TrimSpace(account.Email))
		if email == "" {
			return nil, fmt.Errorf("gmail account email is empty")
		}
		if _, exists := connector.watched[email]; exists {
			return nil, fmt.Errorf("gmail account %s is configured more than once", email)
		}
		account.Email = email
		client, err := factory(account)
		if err != nil {
			return nil, fmt.Errorf("gmail client for %s: %w", email, err)
		}
		connector.watched[email] = &gmailWatched{config: account, client: client, sentID: make(map[string]struct{})}
		connector.accountOrder = append(connector.accountOrder, email)
	}
	return connector, nil
}

func (g *GmailConnector) Name() string { return GmailName }

func (g *GmailConnector) ManifestTools() []ToolDef {
	return append([]ToolDef(nil), gmailTools...)
}

func (g *GmailConnector) Instructions() string { return gmailInstructions }

func (g *GmailConnector) Accounts() []string {
	return append([]string(nil), g.accountOrder...)
}

func (g *GmailConnector) CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
	if name != "gmail_reply" && name != "gmail_send" {
		return ToolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
	if refusal := g.shadow.Refusal(name); refusal != nil {
		return *refusal, nil
	}
	sendArgs := gmailSendArgs{
		Account: gmailToolString(args, "account"), To: gmailToolString(args, "to"),
		Subject: gmailToolString(args, "subject"), Body: gmailToolString(args, "body"),
		CC: gmailToolString(args, "cc"), ThreadID: gmailToolString(args, "thread_id"),
	}
	if name == "gmail_reply" && sendArgs.ThreadID == "" {
		return ToolResult{Status: 400, Text: "gmail_reply requires thread_id — use gmail_send to start a new thread", IsError: true}, nil
	}
	if name == "gmail_send" {
		sendArgs.ThreadID = ""
	}
	sent, err := g.sendMail(ctx, sendArgs)
	if err != nil {
		return ToolResult{}, err
	}
	text := "sent (gmail_id=" + sent.ID
	if sent.ThreadID != "" {
		text += ", thread=" + sent.ThreadID
	}
	return ToolResult{Text: text + ")"}, nil
}

func (g *GmailConnector) PostReply(ctx context.Context, delivery DeliveryContext, message string) error {
	var meta map[string]string
	if err := json.Unmarshal([]byte(delivery.Event.MetaJSON), &meta); err != nil {
		return fmt.Errorf("event %d has invalid meta_json: %w", delivery.Event.ID, err)
	}
	account := meta["account"]
	to := meta["from_email"]
	if account == "" {
		return fmt.Errorf("event %d has no meta.account — cannot decide which mailbox replies", delivery.Event.ID)
	}
	if to == "" {
		return fmt.Errorf("event %d has no meta.from_email — nobody to reply to", delivery.Event.ID)
	}
	_, err := g.sendMail(ctx, gmailSendArgs{Account: account, To: to, Body: message, ThreadID: delivery.ConversationID})
	return err
}

func (g *GmailConnector) Start(ctx context.Context) error {
	g.lifecycleMu.Lock()
	if g.started {
		g.lifecycleMu.Unlock()
		return fmt.Errorf("gmail connector is already started")
	}
	loopContext, cancel := context.WithCancel(ctx)
	g.started = true
	g.cancel = cancel
	g.lifecycleMu.Unlock()

	for _, account := range g.accountOrder {
		watched := g.watched[account]
		profile, err := watched.client.GetProfile(ctx)
		if err != nil {
			g.log("%s: startup check FAILED — %v — polling anyway, with backoff", account, err)
		} else {
			if !strings.EqualFold(profile.EmailAddress, account) {
				g.log("WARN: %s: token authenticates as %s — check token_command", account, profile.EmailAddress)
			}
			watermark, loadErr := g.store.GetWatermark(account)
			if loadErr != nil {
				cancel()
				return loadErr
			}
			if watermark == nil {
				if err := g.store.SetWatermark(account, profile.HistoryID, g.now()); err != nil {
					cancel()
					return err
				}
				g.log("%s: token OK — first run, watermark bootstrapped at historyId %s (existing inbox is not delivered)", account, profile.HistoryID)
			} else {
				g.log("%s: token OK — resuming from historyId %s", account, *watermark)
			}
		}
		g.loops.Add(1)
		go g.pollForever(loopContext, watched)
	}
	g.log("polling %d gmail account(s) every ~%s (jittered)", len(g.watched), g.pollInterval)
	return nil
}

func (g *GmailConnector) Stop(ctx context.Context) error {
	g.lifecycleMu.Lock()
	if !g.started {
		g.lifecycleMu.Unlock()
		return nil
	}
	cancel := g.cancel
	g.lifecycleMu.Unlock()
	cancel()
	done := make(chan struct{})
	go func() {
		g.loops.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		g.lifecycleMu.Lock()
		g.started = false
		g.cancel = nil
		g.lifecycleMu.Unlock()
		return nil
	}
}

func (g *GmailConnector) PollOnce(ctx context.Context, account string) error {
	watched, err := g.require(account)
	if err != nil {
		return err
	}
	watched.pollMu.Lock()
	defer watched.pollMu.Unlock()
	watermark, err := g.store.GetWatermark(watched.config.Email)
	if err != nil {
		return err
	}
	if watermark == nil {
		return g.bootstrapWatermark(ctx, watched, "no watermark")
	}
	ids := make([]string, 0)
	seen := make(map[string]struct{})
	latest := *watermark
	pageToken := ""
	for {
		page, err := watched.client.HistoryList(ctx, *watermark, pageToken)
		if err != nil {
			var httpError *GmailHTTPError
			if errors.As(err, &httpError) && httpError.Status == 404 {
				g.log("%s WARN: historyId watermark expired (404) — re-bootstrapping; mail that arrived during the gap is NOT delivered", watched.config.Email)
				return g.bootstrapWatermark(ctx, watched, "watermark expired")
			}
			return err
		}
		for _, record := range page.History {
			for _, added := range record.MessagesAdded {
				id := added.Message.ID
				if _, exists := seen[id]; !exists {
					seen[id] = struct{}{}
					ids = append(ids, id)
				}
			}
		}
		if page.HistoryID != "" {
			latest = page.HistoryID
		}
		pageToken = page.NextPageToken
		if pageToken == "" {
			break
		}
	}
	for _, id := range ids {
		if err := g.ingestMessage(ctx, watched, id); err != nil {
			return err
		}
	}
	// Advance only after every id is durable. A crash mid-pass replays, and the
	// account-qualified unique event key absorbs those duplicates.
	if latest != *watermark {
		return g.store.SetWatermark(watched.config.Email, latest, g.now())
	}
	return nil
}

func (g *GmailConnector) pollForever(ctx context.Context, watched *gmailWatched) {
	defer g.loops.Done()
	interval := g.pollInterval
	if watched.config.PollSeconds > 0 {
		interval = time.Duration(watched.config.PollSeconds * float64(time.Second))
	}
	consecutiveErrors := 0
	for ctx.Err() == nil {
		if err := g.PollOnce(ctx, watched.config.Email); err != nil {
			if ctx.Err() != nil {
				return
			}
			consecutiveErrors++
			g.log("%s poll failed (%d in a row): %v", watched.config.Email, consecutiveErrors, err)
		} else {
			consecutiveErrors = 0
		}
		delay := interval
		if consecutiveErrors != 0 {
			multiplier := 1 << min(consecutiveErrors, 4)
			delay = min(time.Duration(multiplier)*interval, gmailMaxPollBackoff)
		}
		if err := g.sleep(ctx, g.jitter(delay)); err != nil {
			return
		}
	}
}

func (g *GmailConnector) bootstrapWatermark(ctx context.Context, watched *gmailWatched, why string) error {
	profile, err := watched.client.GetProfile(ctx)
	if err != nil {
		return err
	}
	if err := g.store.SetWatermark(watched.config.Email, profile.HistoryID, g.now()); err != nil {
		return err
	}
	g.log("%s watermark bootstrapped at historyId %s (%s) — existing mail is not delivered", watched.config.Email, profile.HistoryID, why)
	return nil
}

func (g *GmailConnector) ingestMessage(ctx context.Context, watched *gmailWatched, gmailID string) error {
	watched.sentMu.Lock()
	_, sent := watched.sentID[gmailID]
	watched.sentMu.Unlock()
	if sent {
		g.log("%s msg %s dropped: sent by this host", watched.config.Email, gmailID)
		return nil
	}
	message, err := watched.client.GetMessage(ctx, gmailID, "full")
	if err != nil {
		var httpError *GmailHTTPError
		if errors.As(err, &httpError) && httpError.Status == 404 {
			g.log("%s msg %s gone before fetch (404) — skipping", watched.config.Email, gmailID)
			return nil
		}
		return err
	}
	filter := GmailFilterOptions{LabelsRequire: watched.config.LabelsRequire, LabelsExclude: watched.config.LabelsExclude}
	normalized := NormalizeGmailMessage(watched.config.Email, message, filter)
	if normalized == nil {
		var headers []GmailHeader
		if message.Payload != nil {
			headers = message.Payload.Headers
		}
		from := ParseGmailAddress(GmailHeaderValue(headers, "From"))
		decision := GmailDeliveryDecision(message, from.Email, watched.config.Email, filter)
		g.log("%s msg %s dropped: %s", watched.config.Email, gmailID, decision.Reason)
		return nil
	}
	content := normalized.Content
	if len(normalized.Attachments) != 0 {
		saved := g.downloadAttachments(ctx, watched, gmailID, normalized.Attachments)
		if len(saved) != 0 {
			content += "\n\n" + FormatGmailAttachmentBlock(saved)
			for _, attachment := range saved {
				if attachment.IsImage && attachment.Path != "" {
					normalized.Meta["image_path"] = attachment.Path
					break
				}
			}
		}
	}
	metaJSON, err := json.Marshal(normalized.Meta)
	if err != nil {
		return err
	}
	rawJSON, err := json.Marshal(map[string]string{
		"account": watched.config.Email, "gmail_id": gmailID, "thread_id": normalized.ThreadID,
	})
	if err != nil {
		return err
	}
	var user *string
	if sender := gmailValueOr(normalized.From.Email, normalized.From.Name); sender != "" {
		user = &sender
	}
	now := g.now()
	event, err := g.store.InsertEvent(EventInsert{
		Connector: GmailName, EventKey: watched.config.Email + ":" + gmailID,
		ConversationID: normalized.ThreadID, User: user, Content: content,
		MetaJSON: string(metaJSON), RawJSON: string(rawJSON),
	}, now)
	if err != nil {
		return err
	}
	if event == nil {
		g.log("%s dup msg %s — already queued, skipping", watched.config.Email, gmailID)
		return nil
	}
	if _, err := g.store.InsertDelivery(event.ID, g.target, now); err != nil {
		return err
	}
	g.log("%s queued msg %s from %s %q as event %d", watched.config.Email, gmailID, gmailValueOr(normalized.From.Email, "(unknown)"), gmailTruncateSubject(normalized.Subject), event.ID)
	return nil
}

func (g *GmailConnector) downloadAttachments(ctx context.Context, watched *gmailWatched, gmailID string, refs []GmailAttachmentRef) []GmailSavedAttachment {
	directory := filepath.Join(g.attachmentDir, gmailSafeName(watched.config.Email), gmailID)
	saved := make([]GmailSavedAttachment, 0, len(refs))
	for _, ref := range refs {
		if ref.Size > gmailMaxAttachmentBytes {
			saved = append(saved, GmailSavedAttachment{Name: ref.Filename, MIME: ref.MIME, Note: fmt.Sprintf("too large to auto-download (%d bytes)", ref.Size)})
			g.log("attachment %s on %s too large — %d bytes; skipping", ref.Filename, gmailID, ref.Size)
			continue
		}
		_, encoded, err := watched.client.GetAttachment(ctx, gmailID, ref.AttachmentID)
		if err != nil {
			g.log("attachment download failed for %s on %s — %v", ref.Filename, gmailID, err)
			saved = append(saved, GmailSavedAttachment{Name: ref.Filename, MIME: ref.MIME, Note: "download failed"})
			continue
		}
		data, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(encoded, "="))
		if err != nil {
			g.log("attachment decode failed for %s on %s — %v", ref.Filename, gmailID, err)
			saved = append(saved, GmailSavedAttachment{Name: ref.Filename, MIME: ref.MIME, Note: "download failed"})
			continue
		}
		if err := os.MkdirAll(directory, 0o755); err != nil {
			g.log("attachment directory failed for %s on %s — %v", ref.Filename, gmailID, err)
			saved = append(saved, GmailSavedAttachment{Name: ref.Filename, MIME: ref.MIME, Note: "download failed"})
			continue
		}
		path := filepath.Join(directory, gmailSafeName(ref.Filename))
		if err := os.WriteFile(path, data, 0o644); err != nil {
			g.log("attachment write failed for %s on %s — %v", ref.Filename, gmailID, err)
			saved = append(saved, GmailSavedAttachment{Name: ref.Filename, MIME: ref.MIME, Note: "download failed"})
			continue
		}
		saved = append(saved, GmailSavedAttachment{Name: ref.Filename, Path: path, MIME: ref.MIME, IsImage: strings.HasPrefix(ref.MIME, "image/")})
	}
	return saved
}

type gmailSendArgs struct {
	Account  string
	To       string
	Subject  string
	Body     string
	CC       string
	ThreadID string
}

func (g *GmailConnector) sendMail(ctx context.Context, args gmailSendArgs) (GmailSentMessage, error) {
	if err := g.shadow.Refuse("sending mail"); err != nil {
		return GmailSentMessage{}, err
	}
	if args.Account == "" || args.To == "" || strings.TrimSpace(args.Body) == "" || (args.Subject == "" && args.ThreadID == "") {
		return GmailSentMessage{}, fmt.Errorf("send requires account, to, a non-empty body, and subject (unless replying with thread_id)")
	}
	watched, err := g.require(args.Account)
	if err != nil {
		return GmailSentMessage{}, err
	}
	threading := GmailReplyHeaderValues{}
	effectiveSubject := args.Subject
	if args.ThreadID != "" {
		thread, metadataErr := watched.client.GetThreadMetadata(ctx, args.ThreadID)
		if metadataErr != nil {
			g.log("%s thread metadata fetch failed for %s — %v", watched.config.Email, args.ThreadID, metadataErr)
		} else if len(thread.Messages) != 0 {
			last := thread.Messages[len(thread.Messages)-1]
			var headers []GmailHeader
			if last.Payload != nil {
				headers = last.Payload.Headers
			}
			threading = GmailReplyHeaders(GmailHeaderValue(headers, "Message-ID"), GmailHeaderValue(headers, "References"))
			if effectiveSubject == "" {
				if subject := GmailHeaderValue(headers, "Subject"); subject != "" {
					effectiveSubject = GmailReplySubject(subject)
				}
			}
		}
	}
	if effectiveSubject == "" {
		return GmailSentMessage{}, fmt.Errorf("could not derive a subject from the thread — pass subject explicitly")
	}
	mime := BuildGmailMIME(GmailMIMEOptions{
		From: watched.config.Email, To: args.To, CC: args.CC, Subject: effectiveSubject,
		Body: args.Body, InReplyTo: threading.InReplyTo, References: threading.References,
	})
	sent, err := watched.client.Send(ctx, GmailBase64URL(mime), args.ThreadID)
	if err != nil {
		return GmailSentMessage{}, err
	}
	watched.sentMu.Lock()
	watched.sentID[sent.ID] = struct{}{}
	watched.sentMu.Unlock()
	g.log("%s sent msg %s to %s", watched.config.Email, sent.ID, args.To)
	return sent, nil
}

func (g *GmailConnector) require(account string) (*gmailWatched, error) {
	account = strings.ToLower(strings.TrimSpace(account))
	watched := g.watched[account]
	if watched == nil {
		return nil, fmt.Errorf("unknown account %s — watched: %s", account, strings.Join(g.accountOrder, ", "))
	}
	return watched, nil
}

func gmailJitter(interval time.Duration, sample float64) time.Duration {
	return time.Duration(float64(interval) * (0.8 + sample*0.4))
}

func gmailSleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func gmailToolString(args map[string]any, name string) string {
	value, _ := args[name].(string)
	return value
}

func gmailSafeName(name string) string {
	name = strings.Map(func(char rune) rune {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '-' || char == '_' {
			return char
		}
		return '_'
	}, name)
	if len(name) > 120 {
		name = name[len(name)-120:]
	}
	if name == "" {
		return "file"
	}
	return name
}

func gmailTruncateSubject(subject string) string {
	runes := []rune(subject)
	if len(runes) > 60 {
		runes = runes[:60]
	}
	return string(runes)
}

func gmailValueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
