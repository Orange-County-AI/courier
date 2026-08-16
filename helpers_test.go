package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type DispatchFakeClock struct {
	now atomic.Int64
}

func dispatchNewFakeClock(now int64) *DispatchFakeClock {
	clock := &DispatchFakeClock{}
	clock.now.Store(now)
	return clock
}

func (c *DispatchFakeClock) Now() int64            { return c.now.Load() }
func (c *DispatchFakeClock) Add(delta int64) int64 { return c.now.Add(delta) }
func (c *DispatchFakeClock) Set(value int64)       { c.now.Store(value) }

type DispatchFakePrompt struct {
	Target  string
	Text    string
	Timeout time.Duration
}

type DispatchFakeStart struct {
	Name      string
	Kind      string
	PaneID    string
	ExtraArgs []string
}

type DispatchFakeRename struct {
	Target string
	Name   string
}

// FakeDriver keeps one ordered log across all methods. Tests should mutate its
// controls before starting goroutines and use the snapshot methods for reads.
type FakeDriver struct {
	mu sync.Mutex

	Calls   []string
	Prompts []DispatchFakePrompt
	Starts  []DispatchFakeStart
	Renames []DispatchFakeRename

	PromptResults []PromptResult
	Agents        []Agent
	GetAgentErr   error
	ListAgentsErr error
	RenameErr     error
	StartErr      error
	StartErrors   []error
	StartResult   *Agent

	RenameDoesNotStick bool
	PromptStarted      chan struct{}
	PromptRelease      <-chan struct{}

	currentPrompts       int
	MaxConcurrentPrompts int
}

func (d *FakeDriver) PromptAgent(ctx context.Context, target, text string, timeout time.Duration) PromptResult {
	d.mu.Lock()
	d.Calls = append(d.Calls, "prompt:"+target)
	d.Prompts = append(d.Prompts, DispatchFakePrompt{Target: target, Text: text, Timeout: timeout})
	d.currentPrompts++
	if d.currentPrompts > d.MaxConcurrentPrompts {
		d.MaxConcurrentPrompts = d.currentPrompts
	}
	result := PromptResult{OK: true}
	if len(d.PromptResults) > 0 {
		result = d.PromptResults[0]
		d.PromptResults = d.PromptResults[1:]
	}
	started := d.PromptStarted
	release := d.PromptRelease
	d.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			result = PromptResult{Code: "cancelled", Error: ctx.Err().Error()}
		}
	}

	d.mu.Lock()
	d.currentPrompts--
	d.mu.Unlock()
	return result
}

func (d *FakeDriver) GetAgent(_ context.Context, target string) (*Agent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Calls = append(d.Calls, "getAgent:"+target)
	if d.GetAgentErr != nil {
		return nil, d.GetAgentErr
	}
	for i := range d.Agents {
		if d.Agents[i].Name == target {
			agent := d.Agents[i]
			return &agent, nil
		}
	}
	return nil, nil
}

func (d *FakeDriver) ListAgents(context.Context) ([]Agent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Calls = append(d.Calls, "listAgents")
	if d.ListAgentsErr != nil {
		return nil, d.ListAgentsErr
	}
	return append([]Agent(nil), d.Agents...), nil
}

func (d *FakeDriver) RenameAgent(_ context.Context, target, name string) (*Agent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Calls = append(d.Calls, "renameAgent:"+target)
	d.Renames = append(d.Renames, DispatchFakeRename{Target: target, Name: name})
	if d.RenameErr != nil {
		return nil, d.RenameErr
	}
	for i := range d.Agents {
		if d.Agents[i].Name != target && d.Agents[i].PaneID != target {
			continue
		}
		agent := d.Agents[i]
		if !d.RenameDoesNotStick {
			d.Agents[i].Name = name
			agent = d.Agents[i]
		}
		return &agent, nil
	}
	return nil, nil
}

func (d *FakeDriver) StartAgent(_ context.Context, name, kind, paneID string, extraArgs []string) (*Agent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Calls = append(d.Calls, "startAgent:"+name)
	d.Starts = append(d.Starts, DispatchFakeStart{Name: name, Kind: kind, PaneID: paneID, ExtraArgs: append([]string(nil), extraArgs...)})
	if len(d.StartErrors) > 0 {
		err := d.StartErrors[0]
		d.StartErrors = d.StartErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if d.StartErr != nil {
		return nil, d.StartErr
	}
	var agent Agent
	if d.StartResult != nil {
		agent = *d.StartResult
	} else {
		agent = Agent{Name: name, Agent: kind, PaneID: paneID, WorkspaceID: strings.SplitN(paneID, ":", 2)[0], Status: "idle", InteractiveReady: true}
		for _, arg := range extraArgs {
			if strings.HasPrefix(arg, "--resume=") {
				agent.Session = &AgentSession{Agent: kind, Kind: "id", Source: "herdr:" + kind, Value: strings.TrimPrefix(arg, "--resume=")}
			}
		}
	}
	if agent.Name == "" {
		agent.Name = name
	}
	if agent.Agent == "" {
		agent.Agent = kind
	}
	if agent.PaneID == "" {
		agent.PaneID = paneID
	}
	d.Agents = append(d.Agents, agent)
	copy := agent
	return &copy, nil
}

func (d *FakeDriver) PaneWaitOutput(context.Context, string, string, time.Duration, bool) bool {
	return false
}

func (d *FakeDriver) SendKeys(context.Context, string, []string) bool { return false }

func (d *FakeDriver) PaneRead(context.Context, string, int) (string, error) {
	return "", errors.New("fake pane read not configured")
}

func (d *FakeDriver) CallLog() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.Calls...)
}

func (d *FakeDriver) PromptLog() []DispatchFakePrompt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DispatchFakePrompt(nil), d.Prompts...)
}

func (d *FakeDriver) StartLog() []DispatchFakeStart {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DispatchFakeStart(nil), d.Starts...)
}

func (d *FakeDriver) RenameLog() []DispatchFakeRename {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DispatchFakeRename(nil), d.Renames...)
}

func (d *FakeDriver) MaxPromptConcurrency() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.MaxConcurrentPrompts
}

type DispatchFakePost struct {
	Context DeliveryContext
	Message string
}

type DispatchFakeConnector struct {
	mu                 sync.Mutex
	ConnectorName      string
	PostFails          bool
	Posts              []DispatchFakePost
	ToolCalls          []string
	UnavailableNotices []DeliveryContext
	UnavailableError   error
}

func (c *DispatchFakeConnector) Name() string {
	if c.ConnectorName == "" {
		return "mattermost"
	}
	return c.ConnectorName
}

func (c *DispatchFakeConnector) ManifestTools() []ToolDef { return nil }
func (c *DispatchFakeConnector) Instructions() string     { return "" }

func (c *DispatchFakeConnector) CallTool(_ context.Context, name string, _ map[string]any) (ToolResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ToolCalls = append(c.ToolCalls, name)
	return ToolResult{Text: "ok"}, nil
}

func (c *DispatchFakeConnector) PostReply(_ context.Context, dc DeliveryContext, message string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Posts = append(c.Posts, DispatchFakePost{Context: dc, Message: message})
	if c.PostFails {
		return errors.New("fake post failed")
	}
	return nil
}

func (c *DispatchFakeConnector) NotifyUnavailable(_ context.Context, dc DeliveryContext) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.UnavailableNotices = append(c.UnavailableNotices, dc)
	return c.UnavailableError
}

func (c *DispatchFakeConnector) UnavailableLog() []DeliveryContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]DeliveryContext(nil), c.UnavailableNotices...)
}

func (c *DispatchFakeConnector) Start(context.Context) error { return nil }
func (c *DispatchFakeConnector) Stop(context.Context) error  { return nil }

type DispatchHarness struct {
	Store      *Store
	Driver     *FakeDriver
	Dispatcher *Dispatcher
	Clock      *DispatchFakeClock
	Connector  *DispatchFakeConnector
	Registry   *Registry
	Target     string
	OrgID      string
}

type DispatchEnqueueInput struct {
	Key            string
	ConversationID string
	User           string
	Content        string
	Meta           map[string]any
}

type DispatchEnqueued struct {
	EventID    int64
	DeliveryID string
}

func dispatchNewHarness(t *testing.T) *DispatchHarness {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "courier.sqlite"),
		WithRedeliverGrace(100),
		WithRedeliverMaxBackoff(8_000),
		WithRedeliverReadFactor(4),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	clock := dispatchNewFakeClock(1_000_000)
	target := "agent-a"
	orgID := "org-a"
	paneID := "w1:p1"
	workspaceID := "w1"
	source := "herdr:claude"
	kind := "id"
	session := "sess-1"
	if _, err := store.PutReconcilerState(ReconcilerStateInput{
		OrgID:               orgID,
		WorkspaceID:         &workspaceID,
		PaneID:              &paneID,
		PaneLabel:           target,
		AgentKind:           "claude",
		NativeSessionSource: &source,
		NativeSessionKind:   &kind,
		NativeSessionValue:  &session,
	}, clock.Now()); err != nil {
		t.Fatalf("PutReconcilerState: %v", err)
	}

	driver := &FakeDriver{Agents: []Agent{{
		Name: target, Agent: "claude", Status: "idle", PaneID: paneID, WorkspaceID: workspaceID,
		Session: &AgentSession{Agent: "claude", Kind: kind, Source: source, Value: session},
	}}}
	connector := &DispatchFakeConnector{ConnectorName: "mattermost"}
	registry := NewRegistry()
	if err := registry.Register(connector); err != nil {
		t.Fatalf("Register: %v", err)
	}
	preview := true
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Store:           store,
		Driver:          driver,
		Target:          target,
		OrgID:           orgID,
		PromptTimeout:   5 * time.Second,
		Now:             clock.Now,
		EnvelopePreview: &preview,
		Connectors:      registry,
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	return &DispatchHarness{
		Store: store, Driver: driver, Dispatcher: dispatcher, Clock: clock,
		Connector: connector, Registry: registry, Target: target, OrgID: orgID,
	}
}

func (h *DispatchHarness) Enqueue(t *testing.T, input DispatchEnqueueInput) DispatchEnqueued {
	t.Helper()
	if input.ConversationID == "" {
		input.ConversationID = "conv-1"
	}
	if input.Content == "" {
		input.Content = input.Key
	}
	meta := "{}"
	if input.Meta != nil {
		encoded, err := json.Marshal(input.Meta)
		if err != nil {
			t.Fatalf("Marshal meta: %v", err)
		}
		meta = string(encoded)
	}
	var user *string
	if input.User != "" {
		user = &input.User
	}
	event, err := h.Store.InsertEvent(EventInsert{
		Connector:      h.Connector.Name(),
		EventKey:       input.Key,
		ConversationID: input.ConversationID,
		User:           user,
		Content:        input.Content,
		MetaJSON:       meta,
		RawJSON:        "{}",
	}, h.Clock.Now())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if event == nil {
		t.Fatalf("duplicate event key %q", input.Key)
	}
	delivery, err := h.Store.InsertDelivery(event.ID, h.Target, h.Clock.Now())
	if err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}
	return DispatchEnqueued{EventID: event.ID, DeliveryID: delivery.ID}
}

func dispatchRestoredAgent(over Agent) Agent {
	agent := Agent{
		Agent:       "claude",
		Status:      "idle",
		PaneID:      "w1:p1",
		WorkspaceID: "w1",
		Session: &AgentSession{
			Agent: "claude", Kind: "id", Source: "herdr:claude", Value: "sess-1",
		},
	}
	if over.Name != "" {
		agent.Name = over.Name
	}
	if over.Agent != "" {
		agent.Agent = over.Agent
	}
	if over.Status != "" {
		agent.Status = over.Status
	}
	if over.PaneID != "" {
		agent.PaneID = over.PaneID
	}
	if over.WorkspaceID != "" {
		agent.WorkspaceID = over.WorkspaceID
	}
	if over.Session != nil {
		agent.Session = over.Session
	}
	return agent
}
