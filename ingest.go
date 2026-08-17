package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// The ingest connector is the generic seam: it turns the courier.ingest/1 wire
// spec (spec/ingest-1.md) into ledger rows, so a third-party integration needs
// no Go code and no rebuild. Every limit below is normative in that document —
// changing one here is a spec change, not a tuning decision.
const (
	IngestName            = "ingest"
	IngestSchema          = "courier.ingest/1"
	IngestSignatureHeader = "Courier-Signature"
	IngestTimestampHeader = "Courier-Timestamp"
	IngestSignaturePrefix = "v1="
	IngestPathPrefix      = "/ingest/"

	ingestMaxBodyBytes      = 256 << 10
	ingestMaxContentBytes   = 64 << 10
	ingestMaxIDChars        = 200
	ingestMaxUserChars      = 200
	ingestMaxMetaEntries    = 32
	ingestMaxMetaKeyChars   = 64
	ingestMaxMetaValueChars = 1024
	ingestMaxInstructions   = 4000
	ingestMinSecretBytes    = 16
	ingestSkewSeconds       = 300
	ingestReplyTimeout      = 10 * time.Second
	ingestReplyKind         = "reply"
)

var (
	ingestSourcePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	// Reserved regardless of which built-ins are active in this process. A source
	// named after a built-in would make the connector attribute on an envelope
	// ambiguous the day the operator turns that built-in on.
	ingestReservedSources = map[string]struct{}{
		MattermostName: {}, GmailName: {}, TelegramName: {}, KaneoName: {},
		IngestName: {}, "host": {}, "courier": {},
	}
)

// IngestSource is one declared integration: the operator's half of the spec.
type IngestSource struct {
	Source       string `json:"source"`
	Secret       string `json:"secret"`
	ReplyURL     string `json:"reply_url,omitempty"`
	Instructions string `json:"instructions,omitempty"`
}

// IngestEvent is the request body. Unknown fields are ignored on purpose: the
// spec promises additive evolution, so a sender written against a later minor
// shape still ingests here.
type IngestEvent struct {
	Schema         string            `json:"schema"`
	EventKey       string            `json:"event_key"`
	ConversationID string            `json:"conversation_id"`
	User           string            `json:"user,omitempty"`
	Trigger        string            `json:"trigger,omitempty"`
	Content        string            `json:"content"`
	Meta           map[string]string `json:"meta,omitempty"`
}

// IngestReplyPayload is the callback body. delivery_id is the receiver's
// idempotency key, and a 2xx answer to this POST is what settles the delivery.
type IngestReplyPayload struct {
	Schema         string `json:"schema"`
	Kind           string `json:"kind"`
	Source         string `json:"source"`
	ConversationID string `json:"conversation_id"`
	EventKey       string `json:"event_key"`
	DeliveryID     string `json:"delivery_id"`
	User           string `json:"user,omitempty"`
	Agent          string `json:"agent"`
	Message        string `json:"message"`
}

type ingestHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// ParseIngestSources validates the whole declaration before any of it is used.
// A partially valid file is refused: an operator who mistyped one secret gets an
// error naming it, not a host that silently drops one integration's events.
func ParseIngestSources(raw string) ([]IngestSource, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("ingest sources declaration is empty")
	}
	var sources []IngestSource
	if err := json.Unmarshal([]byte(trimmed), &sources); err != nil {
		return nil, fmt.Errorf("parse ingest sources: %w", err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("ingest sources declaration lists no sources")
	}
	seen := make(map[string]struct{}, len(sources))
	parsed := make([]IngestSource, 0, len(sources))
	for index, source := range sources {
		name := strings.TrimSpace(source.Source)
		if !ingestSourcePattern.MatchString(name) {
			return nil, fmt.Errorf(
				"ingest source #%d has name %q; names must match %s",
				index+1, source.Source, ingestSourcePattern.String(),
			)
		}
		if _, reserved := ingestReservedSources[name]; reserved {
			return nil, fmt.Errorf("ingest source %q is a reserved connector name", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("ingest source %q is declared more than once", name)
		}
		seen[name] = struct{}{}
		secret := strings.TrimSpace(source.Secret)
		if len(secret) < ingestMinSecretBytes {
			return nil, fmt.Errorf(
				"ingest source %q needs a secret of at least %d bytes",
				name, ingestMinSecretBytes,
			)
		}
		replyURL := strings.TrimSpace(source.ReplyURL)
		if replyURL != "" {
			parsedURL, err := url.Parse(replyURL)
			if err != nil {
				return nil, fmt.Errorf("ingest source %q has an unparseable reply_url: %w", name, err)
			}
			if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
				return nil, fmt.Errorf("ingest source %q has reply_url scheme %q; use http or https", name, parsedURL.Scheme)
			}
			if parsedURL.Host == "" {
				return nil, fmt.Errorf("ingest source %q has a reply_url with no host", name)
			}
		}
		instructions := strings.TrimSpace(source.Instructions)
		if len(instructions) > ingestMaxInstructions {
			return nil, fmt.Errorf(
				"ingest source %q has %d characters of instructions; the limit is %d",
				name, len(instructions), ingestMaxInstructions,
			)
		}
		parsed = append(parsed, IngestSource{
			Source: name, Secret: secret, ReplyURL: replyURL, Instructions: instructions,
		})
	}
	return parsed, nil
}

// LoadIngestSources mirrors the Gmail accounts loader: the inline value wins,
// the file is the secret-mount path, and "not enabled" is not an error.
func LoadIngestSources(options IngestOptions) ([]IngestSource, bool, error) {
	if !options.Enabled {
		return nil, false, nil
	}
	source := strings.TrimSpace(options.SourcesJSON)
	if source == "" && strings.TrimSpace(options.SourcesFile) != "" {
		contents, err := os.ReadFile(options.SourcesFile)
		if err != nil {
			return nil, true, fmt.Errorf("read COURIER_INGEST_SOURCES_FILE: %w", err)
		}
		source = string(contents)
	}
	if source == "" {
		return nil, true, fmt.Errorf(
			"COURIER_INGEST_SOURCES_JSON or COURIER_INGEST_SOURCES_FILE is required when COURIER_INGEST_LISTEN_PORT is set",
		)
	}
	sources, err := ParseIngestSources(source)
	if err != nil {
		return nil, true, err
	}
	return sources, true, nil
}

type IngestHostConfig struct {
	Port   int
	Store  *Store
	Target string
	Shadow ShadowMode
	Client ingestHTTPClient
	Now    func() int64
	Log    func(string, ...any)
}

// IngestHost owns the one listener every declared source shares. Sources are
// separate connectors — the agent sees the source name as connector= — so the
// listener is reference counted by their Start/Stop instead of belonging to one.
type IngestHost struct {
	port   int
	store  *Store
	target string
	shadow ShadowMode
	client ingestHTTPClient
	now    func() int64
	log    func(string, ...any)

	mu       sync.Mutex
	routes   map[string]*IngestConnector
	order    []string
	server   *http.Server
	listener net.Listener
	refs     int
}

func NewIngestHost(config IngestHostConfig) (*IngestHost, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("ingest host requires a store")
	}
	if strings.TrimSpace(config.Target) == "" {
		return nil, fmt.Errorf("ingest host requires a target")
	}
	if config.Port < 1 || config.Port > 65535 {
		return nil, fmt.Errorf("ingest host requires a listen port, got %d", config.Port)
	}
	client := config.Client
	if client == nil {
		client = http.DefaultClient
	}
	now := config.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	log := config.Log
	if log == nil {
		log = func(string, ...any) {}
	}
	return &IngestHost{
		port:   config.Port,
		store:  config.Store,
		target: strings.TrimSpace(config.Target),
		shadow: config.Shadow,
		client: client,
		now:    now,
		log:    log,
		routes: make(map[string]*IngestConnector),
	}, nil
}

// Add builds the connector for one declared source and claims its route.
func (h *IngestHost) Add(source IngestSource) (*IngestConnector, error) {
	if !ingestSourcePattern.MatchString(source.Source) {
		return nil, fmt.Errorf("ingest source name %q is invalid", source.Source)
	}
	if len(source.Secret) < ingestMinSecretBytes {
		return nil, fmt.Errorf("ingest source %q needs a secret of at least %d bytes", source.Source, ingestMinSecretBytes)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.routes[source.Source]; exists {
		return nil, fmt.Errorf("ingest source %q is already registered", source.Source)
	}
	connector := &IngestConnector{host: h, source: source}
	h.routes[source.Source] = connector
	h.order = append(h.order, source.Source)
	return connector, nil
}

func (h *IngestHost) Sources() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.order...)
}

func (h *IngestHost) route(source string) *IngestConnector {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.routes[source]
}

func (h *IngestHost) Address() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// start binds on the first source's Start and counts the rest. serve.go starts
// and stops every connector, so the count is what keeps one shared listener
// alive exactly as long as at least one source is active.
func (h *IngestHost) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.server != nil {
		h.refs++
		return nil
	}
	// Loopback is a capability boundary: senders reach this listener through an
	// operator-provided tunnel or proxy, and courier never opens it itself.
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(h.port)))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: h}
	h.listener = listener
	h.server = server
	h.refs = 1
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.log("ingest listener failed: %v", err)
		}
	}()
	h.log("ingest listening on %s%s{source} for %s", listener.Addr(), IngestPathPrefix, strings.Join(h.order, ", "))
	return nil
}

// stop shuts the listener down once the last source stops. Shutdown rather than
// Close: a sender is told 202 only after its handler committed the row, so
// in-flight handlers must drain.
func (h *IngestHost) stop(ctx context.Context) error {
	h.mu.Lock()
	if h.server == nil {
		h.mu.Unlock()
		return nil
	}
	h.refs--
	if h.refs > 0 {
		h.mu.Unlock()
		return nil
	}
	server := h.server
	h.server = nil
	h.listener = nil
	h.refs = 0
	h.mu.Unlock()
	return server.Shutdown(ctx)
}

func (h *IngestHost) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/health" {
		writeIngestJSON(w, http.StatusOK, map[string]any{
			"ok": true, "connector": IngestName, "schema": IngestSchema, "sources": h.Sources(),
		})
		return
	}
	if request.Method != http.MethodPost || !strings.HasPrefix(request.URL.Path, IngestPathPrefix) {
		writeIngestReject(w, http.StatusNotFound, "POST "+IngestPathPrefix+"{source} is the only ingest endpoint")
		return
	}
	name := strings.Trim(strings.TrimPrefix(request.URL.Path, IngestPathPrefix), "/")
	if name == "" {
		writeIngestReject(w, http.StatusNotFound, "POST "+IngestPathPrefix+"{source} needs a source name")
		return
	}
	connector := h.route(name)
	if connector == nil {
		// Answered before any signature check because there is no secret to check
		// with. spec/ingest-1.md §10 records the enumeration tradeoff.
		h.log("ingest drop: unknown source %q", name)
		writeIngestReject(w, http.StatusNotFound, "no source named "+strconv.Quote(name)+" is configured")
		return
	}
	connector.serveIngest(w, request)
}

// IngestConnector is one source. It declares no MCP tools: an integration gets
// the agent's attention and the host tools, never a new verb in the agent's
// hands that courier cannot reason about.
type IngestConnector struct {
	host   *IngestHost
	source IngestSource
}

func (c *IngestConnector) Name() string { return c.source.Source }

func (c *IngestConnector) ManifestTools() []ToolDef { return nil }

func (c *IngestConnector) Instructions() string {
	name := c.source.Source
	var builder strings.Builder
	builder.WriteString("Events from the ")
	builder.WriteString(name)
	builder.WriteString(" integration arrive with connector=\"")
	builder.WriteString(name)
	builder.WriteString("\"; its conversation_id is opaque and is passed back unchanged.")
	if c.source.ReplyURL == "" {
		builder.WriteString(" This source is one-way: it accepts no replies, chat_reply is refused for it, " +
			"and its messages are settled with mark_handled once you have acted on them.")
	} else {
		builder.WriteString(" chat_reply sends your answer back to " + name + " over its integration endpoint.")
	}
	if c.source.Instructions != "" {
		builder.WriteString(" ")
		builder.WriteString(c.source.Instructions)
	}
	return builder.String()
}

func (c *IngestConnector) CallTool(context.Context, string, map[string]any) (ToolResult, error) {
	return ToolResult{}, fmt.Errorf("connector %s declares no tools", c.source.Source)
}

func (c *IngestConnector) Start(ctx context.Context) error { return c.host.start(ctx) }

func (c *IngestConnector) Stop(ctx context.Context) error { return c.host.stop(ctx) }

// RefuseReply keeps a one-way source out of the retry loop. Refusing at the tool
// boundary leaves no recorded reply, so nothing is retried forever and the agent
// is told the one thing it can still do.
func (c *IngestConnector) RefuseReply(DeliveryContext) string {
	if c.source.ReplyURL != "" {
		return ""
	}
	return "source " + c.source.Source + " accepts no replies — it declares no reply endpoint, so nothing you " +
		"pass to chat_reply could reach anyone. Settle this message with mark_handled instead."
}

// PostReply returns nil only for a 2xx from the source's reply endpoint. That
// 2xx is what settles the delivery, so anything else — including a timeout — has
// to stay an error for RetryPosts to own.
func (c *IngestConnector) PostReply(ctx context.Context, dc DeliveryContext, message string) error {
	if c.source.ReplyURL == "" {
		return fmt.Errorf("source %s declares no reply_url", c.source.Source)
	}
	if err := c.host.shadow.Refuse("posting a reply to " + c.source.Source); err != nil {
		return err
	}
	body := strings.TrimSpace(message)
	if body == "" {
		return fmt.Errorf("refusing to post an empty reply to %s", c.source.Source)
	}
	user := ""
	if dc.Event.User != nil {
		user = *dc.Event.User
	}
	payload, err := json.Marshal(IngestReplyPayload{
		Schema:         IngestSchema,
		Kind:           ingestReplyKind,
		Source:         c.source.Source,
		ConversationID: dc.ConversationID,
		EventKey:       dc.Event.EventKey,
		DeliveryID:     dc.Delivery.ID,
		User:           user,
		Agent:          dc.Delivery.Target,
		Message:        body,
	})
	if err != nil {
		return err
	}
	postCtx, cancel := context.WithTimeout(ctx, ingestReplyTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(postCtx, http.MethodPost, c.source.ReplyURL, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	timestamp := c.host.now() / 1000
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(IngestTimestampHeader, strconv.FormatInt(timestamp, 10))
	request.Header.Set(IngestSignatureHeader, SignIngest(c.source.Secret, timestamp, payload))
	response, err := c.host.client.Do(request)
	if err != nil {
		return fmt.Errorf("post reply to %s: %w", c.source.Source, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 300))
		return fmt.Errorf("%s reply endpoint: HTTP %d %s", c.source.Source, response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	c.host.log("delivered reply for %s to %s", dc.Delivery.ID, c.source.Source)
	return nil
}

func (c *IngestConnector) serveIngest(w http.ResponseWriter, request *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(request.Body, ingestMaxBodyBytes+1))
	if err != nil {
		writeIngestJSON(w, http.StatusInternalServerError, map[string]any{
			"schema": IngestSchema, "status": "error", "error": "could not read the request body",
		})
		return
	}
	if len(raw) > ingestMaxBodyBytes {
		writeIngestReject(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("body exceeds %d bytes", ingestMaxBodyBytes))
		return
	}
	// Authenticate the exact bytes before parsing or touching sqlite, so an
	// unauthenticated body is never work courier performs.
	timestamp, err := ingestTimestamp(request.Header.Get(IngestTimestampHeader))
	if err != nil {
		c.host.log("ingest drop: %s from %s", err, c.source.Source)
		writeIngestReject(w, http.StatusUnauthorized, err.Error())
		return
	}
	if skew := c.host.now()/1000 - timestamp; skew > ingestSkewSeconds || skew < -ingestSkewSeconds {
		c.host.log("ingest drop: %s timestamp is %ds off", c.source.Source, skew)
		writeIngestReject(w, http.StatusUnauthorized,
			fmt.Sprintf("%s is outside the %d second window", IngestTimestampHeader, ingestSkewSeconds))
		return
	}
	if !VerifyIngestSignature(c.source.Secret, request.Header.Get(IngestSignatureHeader), timestamp, raw) {
		c.host.log("ingest drop: bad signature from %s", c.source.Source)
		writeIngestReject(w, http.StatusUnauthorized, IngestSignatureHeader+" does not match the signed payload")
		return
	}

	var event IngestEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		writeIngestReject(w, http.StatusBadRequest, "body is not a "+IngestSchema+" object: "+err.Error())
		return
	}
	if err := event.validate(); err != nil {
		writeIngestReject(w, http.StatusBadRequest, err.Error())
		return
	}

	stored, delivery, err := c.ingest(event, raw)
	if err != nil {
		c.host.log("ingest failed for %s: %v", c.source.Source, err)
		writeIngestJSON(w, http.StatusInternalServerError, map[string]any{
			"schema": IngestSchema, "status": "error", "error": "could not persist the event",
		})
		return
	}
	if stored == nil {
		c.host.log("dup event_key %s from %s — already queued", event.EventKey, c.source.Source)
		writeIngestJSON(w, http.StatusOK, map[string]any{
			"schema": IngestSchema, "status": "duplicate",
			"source": c.source.Source, "event_key": event.EventKey,
		})
		return
	}
	c.host.log("queued %s event %s as event %d", c.source.Source, event.EventKey, stored.ID)
	writeIngestJSON(w, http.StatusAccepted, map[string]any{
		"schema": IngestSchema, "status": "queued",
		"source": c.source.Source, "event_key": event.EventKey,
		"event_id": stored.ID, "delivery_id": delivery.ID,
	})
}

// ingest commits the event and its delivery inline, which is what makes the 202
// a durable receipt rather than an intention.
func (c *IngestConnector) ingest(event IngestEvent, raw []byte) (*Event, *Delivery, error) {
	meta := make(map[string]string, len(event.Meta)+1)
	for key, value := range event.Meta {
		meta[key] = value
	}
	if event.Trigger != "" {
		meta["trigger"] = event.Trigger
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, nil, err
	}
	var user *string
	if event.User != "" {
		sender := event.User
		user = &sender
	}
	now := c.host.now()
	stored, err := c.host.store.InsertEvent(EventInsert{
		Connector:      c.source.Source,
		EventKey:       event.EventKey,
		ConversationID: event.ConversationID,
		User:           user,
		Content:        event.Content,
		MetaJSON:       string(metaJSON),
		RawJSON:        string(raw),
	}, now)
	if err != nil {
		return nil, nil, err
	}
	if stored == nil {
		return nil, nil, nil
	}
	delivery, err := c.host.store.InsertDelivery(stored.ID, c.host.target, now)
	if err != nil {
		return nil, nil, err
	}
	return stored, delivery, nil
}

func (e IngestEvent) validate() error {
	if strings.TrimSpace(e.Schema) != IngestSchema {
		return fmt.Errorf("schema must be %q, got %q", IngestSchema, e.Schema)
	}
	if err := ingestRequiredID("event_key", e.EventKey); err != nil {
		return err
	}
	if err := ingestRequiredID("conversation_id", e.ConversationID); err != nil {
		return err
	}
	if strings.TrimSpace(e.Content) == "" {
		return fmt.Errorf("content is required and carries the whole message the agent reads")
	}
	if len(e.Content) > ingestMaxContentBytes {
		return fmt.Errorf("content is %d bytes; the limit is %d — link to the rest", len(e.Content), ingestMaxContentBytes)
	}
	if utf8.RuneCountInString(e.User) > ingestMaxUserChars {
		return fmt.Errorf("user exceeds %d characters", ingestMaxUserChars)
	}
	if utf8.RuneCountInString(e.Trigger) > MaxTriggerChars {
		return fmt.Errorf("trigger exceeds %d characters", MaxTriggerChars)
	}
	if len(e.Meta) > ingestMaxMetaEntries {
		return fmt.Errorf("meta has %d entries; the limit is %d", len(e.Meta), ingestMaxMetaEntries)
	}
	// One spelling per fact: trigger is a top-level field, and courier is the
	// only writer of meta.trigger, which the envelope reads.
	if _, reserved := e.Meta["trigger"]; reserved {
		return fmt.Errorf("meta.trigger is reserved; use the top-level trigger field")
	}
	for key, value := range e.Meta {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("meta has an empty key")
		}
		if utf8.RuneCountInString(key) > ingestMaxMetaKeyChars {
			return fmt.Errorf("meta key %q exceeds %d characters", key, ingestMaxMetaKeyChars)
		}
		if utf8.RuneCountInString(value) > ingestMaxMetaValueChars {
			return fmt.Errorf("meta value for %q exceeds %d characters", key, ingestMaxMetaValueChars)
		}
	}
	return nil
}

func ingestRequiredID(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	if utf8.RuneCountInString(value) > ingestMaxIDChars {
		return fmt.Errorf("%s exceeds %d characters", field, ingestMaxIDChars)
	}
	return nil
}

// IngestSignedPayload is the canonical string both sides sign: the timestamp as
// transmitted, a dot, then the exact body bytes. Senders must sign the bytes
// they transmit — re-serializing after signing is the common integration bug.
func IngestSignedPayload(timestamp int64, body []byte) []byte {
	stamp := strconv.FormatInt(timestamp, 10)
	payload := make([]byte, 0, len(stamp)+1+len(body))
	payload = append(payload, stamp...)
	payload = append(payload, '.')
	return append(payload, body...)
}

func SignIngest(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(IngestSignedPayload(timestamp, body))
	return IngestSignaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

func VerifyIngestSignature(secret, header string, timestamp int64, body []byte) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	expected := SignIngest(secret, timestamp, body)
	// ConstantTimeCompare returns 0 on unequal lengths, so a malformed header is
	// an ordinary authentication failure rather than a distinct path.
	return subtle.ConstantTimeCompare([]byte(header), []byte(expected)) == 1
}

func ingestTimestamp(header string) (int64, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0, fmt.Errorf("%s is required", IngestTimestampHeader)
	}
	timestamp, err := strconv.ParseInt(header, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be integer unix seconds", IngestTimestampHeader)
	}
	return timestamp, nil
}

func writeIngestReject(w http.ResponseWriter, status int, message string) {
	writeIngestJSON(w, status, map[string]any{
		"schema": IngestSchema, "status": "rejected", "error": message,
	})
}

func writeIngestJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
