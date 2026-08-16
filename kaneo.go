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
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	KaneoName      = "kaneo"
	KaneoBotMarker = "🤖"
)

var kaneoTools = []ToolDef{
	{
		Name: "kaneo_comment",
		Description: "Post a comment on a Kaneo task. For answering a Kaneo event you were handed, use chat_reply — it " +
			"comments on the right task already. This is for commenting on a DIFFERENT task, or a second time.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"task_id": map[string]any{"type": "string", "description": "The task's id (meta.task_id on the event), not its number."},
				"message": map[string]any{"type": "string", "description": "The comment body, markdown."},
			},
			"required": []string{"task_id", "message"},
		},
	},
}

const kaneoInstructions = "Kaneo task events arrive with connector=\"kaneo\" and conversation_id=\"kaneo:<task id>\". chat_reply posts " +
	"your answer as a comment on that task. Comments this host posts are prefixed with a 🤖 marker and are " +
	"filtered out of ingestion, so you will never be handed your own comment back — you do not have to check."

type KaneoActor struct {
	ID   *string `json:"id,omitempty"`
	Name *string `json:"name,omitempty"`
}

type KaneoProject struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	WorkspaceID string `json:"workspaceId,omitempty"`
}

type KaneoTask struct {
	ID         string `json:"id,omitempty"`
	Number     *int64 `json:"number,omitempty"`
	Title      string `json:"title,omitempty"`
	Status     string `json:"status,omitempty"`
	StatusName string `json:"statusName,omitempty"`
	Priority   string `json:"priority,omitempty"`
	URL        string `json:"url,omitempty"`
}

type KaneoWebhook struct {
	Event       string         `json:"event,omitempty"`
	Timestamp   string         `json:"timestamp,omitempty"`
	Integration map[string]any `json:"integration,omitempty"`
	Project     *KaneoProject  `json:"project,omitempty"`
	Task        *KaneoTask     `json:"task,omitempty"`
	Actor       *KaneoActor    `json:"actor,omitempty"`
	Data        map[string]any `json:"data,omitempty"`
}

type KaneoNormalized struct {
	EventType   string
	Action      string
	Task        string
	TaskID      string
	Project     string
	WorkspaceID string
	Actor       string
	Content     string
	Meta        map[string]string
}

type kaneoHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type KaneoConnectorConfig struct {
	Store   *Store
	Target  string
	Options KaneoOptions
	Shadow  ShadowMode
	Client  kaneoHTTPClient
	Now     func() int64
	Log     func(string, ...any)
}

type KaneoConnector struct {
	store  *Store
	target string
	opts   KaneoOptions
	shadow ShadowMode
	client kaneoHTTPClient
	now    func() int64
	log    func(string, ...any)

	serverMu sync.Mutex
	server   *http.Server
	listener net.Listener
}

func NewKaneoConnector(config KaneoConnectorConfig) (*KaneoConnector, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("kaneo connector requires a store")
	}
	if strings.TrimSpace(config.Target) == "" {
		return nil, fmt.Errorf("kaneo connector requires a target")
	}
	missing := make([]string, 0, 3)
	if strings.TrimSpace(config.Options.WebhookSecret) == "" {
		missing = append(missing, "KANEO_CHANNEL_WEBHOOK_SECRET")
	}
	if strings.TrimSpace(config.Options.APIBase) == "" {
		missing = append(missing, "KANEO_API_BASE")
	}
	if strings.TrimSpace(config.Options.BotKey) == "" {
		missing = append(missing, "KANEO_BOT_KEY")
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("kaneo connector is partially configured — missing %s", strings.Join(missing, ", "))
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
	return &KaneoConnector{
		store:  config.Store,
		target: strings.TrimSpace(config.Target),
		opts:   config.Options,
		shadow: config.Shadow,
		client: client,
		now:    now,
		log:    log,
	}, nil
}

func (k *KaneoConnector) Name() string { return KaneoName }

func (k *KaneoConnector) ManifestTools() []ToolDef {
	return append([]ToolDef(nil), kaneoTools...)
}

func (k *KaneoConnector) Instructions() string { return kaneoInstructions }

func (k *KaneoConnector) CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error) {
	if name != "kaneo_comment" {
		return ToolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
	if refusal := k.shadow.Refusal("kaneo_comment"); refusal != nil {
		return *refusal, nil
	}
	taskID, _ := args["task_id"].(string)
	message, _ := args["message"].(string)
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || strings.TrimSpace(message) == "" {
		return ToolResult{Status: 400, Text: "kaneo_comment requires task_id and a non-empty message", IsError: true}, nil
	}
	if err := k.comment(ctx, taskID, message); err != nil {
		return ToolResult{}, err
	}
	return ToolResult{Text: "commented on task " + taskID}, nil
}

// PostReply is the handled gate: nil means Kaneo confirmed the comment with a
// 2xx response. Every other outcome remains an error for hosttools to retain.
func (k *KaneoConnector) PostReply(ctx context.Context, dc DeliveryContext, message string) error {
	conversationID := dc.ConversationID
	taskID := conversationID
	if strings.HasPrefix(conversationID, KaneoName+":") {
		taskID = strings.TrimPrefix(conversationID, KaneoName+":")
	}
	if taskID == "" {
		return fmt.Errorf("conversation_id %q names no task", conversationID)
	}
	if err := k.comment(ctx, taskID, message); err != nil {
		return err
	}
	k.log("commented on task %s", taskID)
	return nil
}

func (k *KaneoConnector) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k.serverMu.Lock()
	defer k.serverMu.Unlock()
	if k.server != nil {
		return fmt.Errorf("kaneo connector is already started")
	}
	// Loopback is a capability boundary. Kaneo is expected to reach this
	// listener through an operator-provided tunnel or reverse proxy; courier
	// never opens the webhook directly to the network.
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(k.opts.ListenPort)))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: k}
	k.listener = listener
	k.server = server
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			k.log("kaneo webhook server failed: %v", err)
		}
	}()
	k.log("kaneo webhook listening on %s/webhook", listener.Addr())
	return nil
}

// Stop uses Shutdown rather than Close: a sender receives 200 only after its
// handler has persisted the event, so in-flight handlers must drain first.
func (k *KaneoConnector) Stop(ctx context.Context) error {
	k.serverMu.Lock()
	server := k.server
	k.serverMu.Unlock()
	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil {
		return err
	}
	k.serverMu.Lock()
	if k.server == server {
		k.server = nil
		k.listener = nil
	}
	k.serverMu.Unlock()
	return nil
}

func (k *KaneoConnector) Address() string {
	k.serverMu.Lock()
	defer k.serverMu.Unlock()
	if k.listener == nil {
		return ""
	}
	return k.listener.Addr().String()
}

func (k *KaneoConnector) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/health" {
		writeKaneoJSON(w, http.StatusOK, map[string]any{"ok": true, "connector": KaneoName})
		return
	}
	if request.Method != http.MethodPost || request.URL.Path != "/webhook" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	// Authenticate the exact bytes before parsing or touching sqlite. Besides
	// avoiding unauthorized work, this keeps malformed signed JSON distinct
	// from an unauthenticated probe.
	if !VerifyKaneoSignature(raw, request.Header.Get("x-kaneo-signature"), k.opts.WebhookSecret) {
		k.log("drop: bad signature")
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var body KaneoWebhook
	if err := json.Unmarshal(raw, &body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// Inline ingest makes 200 a durable receipt. Deferring this work would tell
	// Kaneo to stop retrying before the row existed.
	if _, err := k.Ingest(body, raw); err != nil {
		k.log("ingest failed: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func (k *KaneoConnector) Ingest(body KaneoWebhook, raw []byte) (*Event, error) {
	normalized := NormalizeKaneo(body)
	if normalized == nil {
		k.log("skip: event %s", body.Event)
		return nil, nil
	}
	// This host serves one org. A foreign workspace is noise rather than a
	// delivery for somebody else; persisting it creates an endless redelivery.
	if k.opts.WorkspaceID != "" && normalized.WorkspaceID != k.opts.WorkspaceID {
		k.log("skip: workspace %s != configured %s", valueOr(normalized.WorkspaceID, "(none)"), k.opts.WorkspaceID)
		return nil, nil
	}
	if normalized.EventType == "Comment" {
		if CarriesKaneoBotMarker(dataString(body.Data, "comment")) {
			k.log("skip: comment carries the bot marker — %s", valueOr(normalized.Task, "(no ref)"))
			return nil, nil
		}
		if k.opts.BotActor != "" && normalized.Actor == k.opts.BotActor {
			k.log("skip: comment authored by the bot itself — %s", valueOr(normalized.Task, "(no ref)"))
			return nil, nil
		}
	}
	if normalized.TaskID == "" {
		k.log("skip: event has no task id — %s %s", normalized.EventType, normalized.Action)
		return nil, nil
	}
	metaJSON, err := json.Marshal(normalized.Meta)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	var user *string
	if normalized.Actor != "" {
		actor := normalized.Actor
		user = &actor
	}
	now := k.now()
	event, err := k.store.InsertEvent(EventInsert{
		Connector:      KaneoName,
		EventKey:       hex.EncodeToString(digest[:]),
		ConversationID: KaneoName + ":" + normalized.TaskID,
		User:           user,
		Content:        normalized.Content,
		MetaJSON:       string(metaJSON),
		RawJSON:        string(raw),
	}, now)
	if err != nil {
		return nil, err
	}
	if event == nil {
		k.log("dup webhook body — %s %s %s — already queued, skipping", normalized.EventType, normalized.Action, valueOr(normalized.Task, "(no ref)"))
		return nil, nil
	}
	if _, err := k.store.InsertDelivery(event.ID, k.target, now); err != nil {
		return nil, err
	}
	k.log("queued %s %s %s as event %d", normalized.EventType, normalized.Action, valueOr(normalized.Task, "(no ref)"), event.ID)
	return event, nil
}

func VerifyKaneoSignature(raw []byte, header, secret string) bool {
	if header == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(raw)
	expected := make([]byte, hex.EncodedLen(mac.Size()))
	hex.Encode(expected, mac.Sum(nil))
	// ConstantTimeCompare returns 0 on unequal lengths, so malformed lengths
	// are ordinary authentication failures rather than a distinct error path.
	return subtle.ConstantTimeCompare([]byte(header), expected) == 1
}

func CarriesKaneoBotMarker(comment string) bool {
	if comment == "" {
		return false
	}
	for line := range strings.SplitSeq(comment, "\n") {
		if strings.HasPrefix(strings.TrimLeftFunc(line, unicode.IsSpace), KaneoBotMarker) {
			return true
		}
	}
	return false
}

func NormalizeKaneo(body KaneoWebhook) *KaneoNormalized {
	if !strings.HasPrefix(body.Event, "task.") {
		return nil
	}
	projectName, workspaceID := "", ""
	if body.Project != nil {
		projectName = body.Project.Name
		workspaceID = body.Project.WorkspaceID
	}
	taskID, title, status, statusName, priority, taskURL := "", "", "", "", "", ""
	if body.Task != nil {
		taskID = body.Task.ID
		title = body.Task.Title
		status = body.Task.Status
		statusName = body.Task.StatusName
		priority = body.Task.Priority
		taskURL = body.Task.URL
	}
	if title == "" {
		title = "(untitled)"
	}
	actor := ""
	if body.Actor != nil && body.Actor.Name != nil {
		actor = *body.Actor.Name
	}
	ref := kaneoTaskRef(body.Project, body.Task)
	isComment := body.Event == "task.comment_created"
	eventType := "Task"
	if isComment {
		eventType = "Comment"
	}
	action := strings.TrimPrefix(body.Event, "task.")
	by := valueOr(actor, "someone")
	refText := valueOr(ref, "(unknown)")
	projectText := valueOr(projectName, "(unknown)")
	urlText := valueOr(taskURL, "(none)")
	var content string
	switch body.Event {
	case "task.created":
		description := strings.TrimSpace(dataString(body.Data, "description"))
		content = fmt.Sprintf("New Kaneo task %s created in project %s by %s.\n\nTitle: %s\n\nDescription:\n%s\n\nStatus: %s  ·  Priority: %s\n\nURL: %s",
			refText, projectText, by, title, valueOr(description, "(no description provided)"), valueOr(valueOr(statusName, status), "(none)"), valueOr(priority, "(none)"), urlText)
	case "task.comment_created":
		comment := strings.TrimSpace(dataString(body.Data, "comment"))
		content = fmt.Sprintf("New comment on Kaneo task %s (project %s) by %s:\n\n%s\n\nTitle: %s\nURL: %s",
			refText, projectText, by, valueOr(comment, "(empty)"), title, urlText)
	case "task.status_changed":
		content = fmt.Sprintf("Kaneo task %s moved from %q to %q by %s.\n\nTitle: %s\nURL: %s",
			refText, valueOr(dataString(body.Data, "oldStatus"), "?"), valueOr(valueOr(dataString(body.Data, "newStatus"), statusName), "?"), by, title, urlText)
	case "task.priority_changed":
		content = fmt.Sprintf("Kaneo task %s priority changed from %q to %q by %s.\n\nTitle: %s\nURL: %s",
			refText, valueOr(dataString(body.Data, "oldPriority"), "?"), valueOr(valueOr(dataString(body.Data, "newPriority"), priority), "?"), by, title, urlText)
	case "task.title_changed":
		content = fmt.Sprintf("Kaneo task %s title changed from %q to %q by %s.\n\nURL: %s",
			refText, valueOr(dataString(body.Data, "oldTitle"), "?"), valueOr(dataString(body.Data, "newTitle"), title), by, urlText)
	case "task.description_changed":
		description := strings.TrimSpace(dataString(body.Data, "newDescription"))
		content = fmt.Sprintf("Kaneo task %s description updated by %s.\n\nTitle: %s\n\nNew description:\n%s\n\nURL: %s",
			refText, by, title, valueOr(description, "(empty)"), urlText)
	default:
		content = fmt.Sprintf("Kaneo task %s — %s by %s.\nTitle: %s\nURL: %s", refText, action, by, title, urlText)
	}
	normalized := &KaneoNormalized{
		EventType: eventType, Action: action, Task: ref, TaskID: taskID,
		Project: projectName, WorkspaceID: workspaceID, Actor: actor, Content: content,
	}
	normalized.Meta = buildKaneoMeta(*normalized, body)
	return normalized
}

func (k *KaneoConnector) comment(ctx context.Context, taskID, message string) error {
	if err := k.shadow.Refuse("commenting on Kaneo"); err != nil {
		return err
	}
	body := strings.TrimSpace(message)
	if body == "" {
		return fmt.Errorf("refusing to post an empty Kaneo comment")
	}
	if !CarriesKaneoBotMarker(body) {
		body = KaneoBotMarker + " " + body
	}
	payload, err := json.Marshal(map[string]string{"taskId": taskID, "comment": body})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(k.opts.APIBase, "/")+"/api/activity/comment", strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-api-key", k.opts.BotKey)
	response, err := k.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 300))
		return fmt.Errorf("kaneo POST /api/activity/comment: HTTP %d %s", response.StatusCode, string(responseBody))
	}
	return nil
}

func buildKaneoMeta(normalized KaneoNormalized, body KaneoWebhook) map[string]string {
	meta := map[string]string{"source": "kaneo-channel", "type": normalized.EventType, "action": normalized.Action}
	putKaneoMeta(meta, "task", normalized.Task)
	putKaneoMeta(meta, "project", normalized.Project)
	putKaneoMeta(meta, "actor", normalized.Actor)
	putKaneoMeta(meta, "slug", slugifyKaneoProject(normalized.Project))
	if body.Task != nil {
		if body.Task.Number != nil {
			meta["number"] = strconv.FormatInt(*body.Task.Number, 10)
		}
		putKaneoMeta(meta, "task_id", body.Task.ID)
		putKaneoMeta(meta, "url", body.Task.URL)
		putKaneoMeta(meta, "status", body.Task.StatusName)
	}
	if body.Project != nil {
		putKaneoMeta(meta, "project_id", body.Project.ID)
		putKaneoMeta(meta, "workspace_id", body.Project.WorkspaceID)
	}
	return meta
}

func putKaneoMeta(meta map[string]string, key, value string) {
	if value != "" {
		meta[key] = value
	}
}

func kaneoTaskRef(project *KaneoProject, task *KaneoTask) string {
	slug := ""
	if project != nil {
		slug = slugifyKaneoProject(project.Name)
	}
	if task != nil && task.Number != nil {
		if slug != "" {
			return slug + "#" + strconv.FormatInt(*task.Number, 10)
		}
		return "#" + strconv.FormatInt(*task.Number, 10)
	}
	return slug
}

func slugifyKaneoProject(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var builder strings.Builder
	pendingDash := false
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			if pendingDash && builder.Len() != 0 {
				builder.WriteByte('-')
			}
			builder.WriteRune(char)
			pendingDash = false
		} else if builder.Len() != 0 {
			pendingDash = true
		}
	}
	return builder.String()
}

func dataString(data map[string]any, key string) string {
	value, _ := data[key].(string)
	return value
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func writeKaneoJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
