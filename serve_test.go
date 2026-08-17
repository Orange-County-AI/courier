package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type serveTestRecorder struct {
	mu    sync.Mutex
	items []string
}

func (r *serveTestRecorder) add(item string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, item)
}

func (r *serveTestRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.items...)
}

type serveTestDriver struct {
	*FakeDriver
	close func() error
}

func (d *serveTestDriver) Close() error {
	if d.close != nil {
		return d.close()
	}
	return nil
}

type serveOrderDriver struct {
	*FakeDriver
	t           *testing.T
	store       *Store
	deliveryID  string
	recorder    *serveTestRecorder
	httpStarted <-chan struct{}
}

func (d *serveOrderDriver) GetAgent(ctx context.Context, target string) (*Agent, error) {
	delivery, err := d.store.GetDelivery(d.deliveryID)
	if err != nil {
		d.t.Errorf("GetDelivery before reconcile: %v", err)
	} else if delivery.Status != DeliveryPending {
		d.t.Errorf("delivery status before first herdr I/O = %s, want %s", delivery.Status, DeliveryPending)
	}
	d.recorder.add("reconcile")
	return d.FakeDriver.GetAgent(ctx, target)
}

func (d *serveOrderDriver) PromptAgent(ctx context.Context, target, text string, timeout time.Duration) PromptResult {
	select {
	case <-d.httpStarted:
	case <-time.After(time.Second):
		d.t.Error("IPC serve did not start before drain")
	}
	d.recorder.add("drain")
	return d.FakeDriver.PromptAgent(ctx, target, text, timeout)
}

func (d *serveOrderDriver) Close() error { return nil }

type serveTestDispatcher struct {
	start  func(context.Context) (ReconcileResult, error)
	drain  func(context.Context) ([]DispatchOutcome, error)
	tick   func(context.Context) ([]DispatchOutcome, error)
	status *TargetStatus
	hold   *DraftHold
}

func (d *serveTestDispatcher) Start(ctx context.Context) (ReconcileResult, error) {
	return d.start(ctx)
}

func (d *serveTestDispatcher) Drain(ctx context.Context) ([]DispatchOutcome, error) {
	return d.drain(ctx)
}

func (d *serveTestDispatcher) Tick(ctx context.Context) ([]DispatchOutcome, error) {
	return d.tick(ctx)
}

func (d *serveTestDispatcher) TargetStatus() *TargetStatus { return d.status }

func (d *serveTestDispatcher) DraftHold() *DraftHold { return d.hold }

type serveTestConnector struct {
	name  string
	start func(context.Context) error
	stop  func(context.Context) error
}

func (c *serveTestConnector) Name() string             { return c.name }
func (c *serveTestConnector) ManifestTools() []ToolDef { return nil }
func (c *serveTestConnector) Instructions() string     { return "" }
func (c *serveTestConnector) CallTool(context.Context, string, map[string]any) (ToolResult, error) {
	return ToolResult{}, errors.New("no test tools")
}
func (c *serveTestConnector) PostReply(context.Context, DeliveryContext, string) error {
	return nil
}
func (c *serveTestConnector) Start(ctx context.Context) error {
	if c.start != nil {
		return c.start(ctx)
	}
	return nil
}
func (c *serveTestConnector) Stop(ctx context.Context) error {
	if c.stop != nil {
		return c.stop(ctx)
	}
	return nil
}

type serveTestHTTPServer struct {
	recorder *serveTestRecorder
	label    string
	started  chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func (s *serveTestHTTPServer) Serve(net.Listener) error {
	label := s.label
	if label == "" {
		label = "http-serve"
	}
	s.recorder.add(label)
	close(s.started)
	<-s.closed
	return http.ErrServerClosed
}

func (s *serveTestHTTPServer) Shutdown(context.Context) error {
	s.recorder.add("ipc-stop")
	s.once.Do(func() { close(s.closed) })
	return nil
}

func serveLazyTestOptions(t *testing.T, shadow bool, socketPath string) ServeOptions {
	t.Helper()
	return ServeOptions{
		Org: "org-test", Target: "agent-test", DBPath: t.TempDir() + "/courier.sqlite",
		Bind: "127.0.0.1", Port: 0, PromptTimeout: 20 * time.Millisecond,
		TickInterval: 0, RedeliverGrace: time.Second, RedeliverMaxBackoff: time.Minute,
		RedeliverReadFactor: 4, Shadow: NewShadowMode(shadow), EnvelopePreview: true,
		Herdr: HerdrOptions{SocketPath: socketPath},
	}
}

func serveTestHealth(t *testing.T, runtime *serveRuntime) healthResponse {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + runtime.listener.Addr().String() + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer response.Body.Close()
	var health healthResponse
	if err := json.NewDecoder(response.Body).Decode(&health); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if response.StatusCode != http.StatusOK || !health.OK {
		t.Fatalf("/health = %d %#v", response.StatusCode, health)
	}
	return health
}

func TestServeConnectorActivationMatrix(t *testing.T) {
	tests := []struct {
		name      string
		allowlist []string
		enabled   map[string]bool
		want      []string
		warnings  []string
	}{
		{
			name:    "presence gates without allowlist",
			enabled: map[string]bool{MattermostName: true, GmailName: false, KaneoName: true},
			want:    []string{MattermostName, KaneoName},
		},
		{
			name:      "allowlist excludes enabled and reports missing requirements",
			allowlist: []string{GmailName},
			enabled:   map[string]bool{MattermostName: true, GmailName: false, KaneoName: true},
			warnings:  []string{"required configuration is absent"},
		},
		{
			name:      "allowlist normalizes names and reports unknown",
			allowlist: []string{" MATTERMOST ", "future-chat"},
			enabled:   map[string]bool{MattermostName: true, GmailName: true, KaneoName: true},
			want:      []string{MattermostName},
			warnings:  []string{"future-chat", "unknown"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs []string
			candidates := make([]serveConnectorCandidate, 0, 3)
			for _, connectorName := range []string{MattermostName, GmailName, KaneoName} {
				connectorName := connectorName
				candidates = append(candidates, serveConnectorCandidate{
					name:    connectorName,
					enabled: test.enabled[connectorName],
					build: func() (Connector, error) {
						return &serveTestConnector{name: connectorName}, nil
					},
				})
			}
			_, active, err := serveActivateConnectors(test.allowlist, candidates, func(format string, args ...any) {
				logs = append(logs, fmt.Sprintf(format, args...))
			})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(active))
			for i, connector := range active {
				got[i] = connector.Name()
			}
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("active connectors = %v, want %v", got, test.want)
			}
			joined := strings.Join(logs, "\n")
			for _, warning := range test.warnings {
				if !strings.Contains(joined, warning) {
					t.Fatalf("logs %q do not contain %q", logs, warning)
				}
			}
		})
	}
}

func TestServeStartupAndGracefulShutdownOrdering(t *testing.T) {
	recorder := &serveTestRecorder{}
	httpStarted := make(chan struct{})
	initialDrain := make(chan struct{})
	connector := &serveTestConnector{
		name: MattermostName,
		start: func(context.Context) error {
			recorder.add("connector-start")
			return nil
		},
		stop: func(context.Context) error {
			recorder.add("connector-stop")
			return nil
		},
	}
	dispatcher := &serveTestDispatcher{
		start: func(context.Context) (ReconcileResult, error) {
			recorder.add("reconcile")
			return ReconcileResult{Action: ReconcileRefreshed}, nil
		},
		drain: func(context.Context) ([]DispatchOutcome, error) {
			<-httpStarted
			recorder.add("drain")
			close(initialDrain)
			return nil, nil
		},
		tick: func(context.Context) ([]DispatchOutcome, error) {
			recorder.add("tick")
			return nil, nil
		},
	}
	driver := &serveTestDriver{FakeDriver: &FakeDriver{}, close: func() error {
		recorder.add("driver-close")
		return nil
	}}

	opts := ServeOptions{
		Org: "org-test", Target: "agent-test", DBPath: t.TempDir() + "/courier.sqlite",
		Bind: "127.0.0.1", Port: 0, PromptTimeout: time.Second, TickInterval: time.Hour,
		RedeliverGrace: time.Second, RedeliverMaxBackoff: time.Minute, RedeliverReadFactor: 4,
		EnvelopePreview: true,
	}
	deps := serveDefaultDependencies()
	defaultOpen := deps.openStore
	deps.openStore = func(options ServeOptions, logf serveLogFunc) (*Store, error) {
		recorder.add("store-open")
		return defaultOpen(options, logf)
	}
	deps.closeStore = func(store *Store) error {
		recorder.add("store-close")
		return store.Close()
	}
	deps.newDriver = func(context.Context, ServeOptions, serveLogFunc) (serveManagedDriver, error) {
		recorder.add("driver-open")
		return driver, nil
	}
	deps.buildConnectors = func(_ *Store, _ ServeOptions, _ serveLogFunc) (*Registry, []Connector, error) {
		recorder.add("connectors-build")
		registry := NewRegistry()
		if err := registry.Register(connector); err != nil {
			return nil, nil, err
		}
		return registry, []Connector{connector}, nil
	}
	deps.newDispatcher = func(*Store, serveManagedDriver, *Registry, ServeOptions, serveLogFunc) (serveDispatcher, error) {
		recorder.add("dispatcher-build")
		return dispatcher, nil
	}
	deps.newHTTPServer = func(http.Handler) serveHTTPServer {
		return &serveTestHTTPServer{recorder: recorder, started: httpStarted, closed: make(chan struct{})}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveWithDependencies(ctx, opts, deps, func(string, ...any) {})
	}()
	select {
	case <-initialDrain:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial drain did not run")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve did not shut down")
	}

	got := recorder.snapshot()
	want := []string{
		"store-open", "driver-open", "connectors-build", "dispatcher-build",
		"reconcile", "connector-start", "http-serve", "drain",
		"connector-stop", "ipc-stop", "driver-close", "store-close",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lifecycle order = %v, want %v", got, want)
	}
}

func TestServeDefaultDriverConstructorDoesNotDial(t *testing.T) {
	t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
	socketPath := t.TempDir() + "/herdr.sock"
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	opts := serveLazyTestOptions(t, false, socketPath)
	driver, err := serveDefaultDependencies().newDriver(context.Background(), opts, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	if err := listener.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn, acceptErr := listener.AcceptUnix()
	if acceptErr == nil {
		_ = conn.Close()
		t.Fatal("default driver constructor dialed herdr before first use")
	}
	var netErr net.Error
	if !errors.As(acceptErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("AcceptUnix error = %v, want deadline with no constructor dial", acceptErr)
	}
}

func TestServeShadowWithMissingHerdrBootsHealthAndIngestsWithoutHerdr(t *testing.T) {
	t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
	socketPath := t.TempDir() + "/missing/herdr.sock"
	opts := serveLazyTestOptions(t, true, socketPath)
	deps := serveDefaultDependencies()
	deps.buildConnectors = func(store *Store, opts ServeOptions, _ serveLogFunc) (*Registry, []Connector, error) {
		connector := &serveTestConnector{
			name: "test-ingest",
			start: func(context.Context) error {
				event, err := store.InsertEvent(EventInsert{
					Connector: "test-ingest", EventKey: "shadow-event", ConversationID: "conversation-1",
					Content: "hello", MetaJSON: "{}", RawJSON: "{}",
				}, time.Now().UnixMilli())
				if err != nil {
					return err
				}
				if event == nil {
					return errors.New("shadow ingestion was deduplicated")
				}
				_, err = store.InsertDelivery(event.ID, opts.Target, time.Now().UnixMilli())
				return err
			},
		}
		registry := NewRegistry()
		if err := registry.Register(connector); err != nil {
			return nil, nil, err
		}
		return registry, []Connector{connector}, nil
	}

	runtime, err := serveAssemble(context.Background(), opts, deps, func(string, ...any) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	if err := runtime.start(context.Background()); err != nil {
		t.Fatalf("shadow start with missing herdr: %v", err)
	}
	health := serveTestHealth(t, runtime)
	if !health.Shadow || health.Events != 1 || !reflect.DeepEqual(health.Connectors, []string{"test-ingest"}) {
		t.Fatalf("shadow health = %#v", health)
	}

	driver, ok := runtime.driver.(*SocketDriver)
	if !ok {
		t.Fatalf("default driver = %T, want *SocketDriver", runtime.driver)
	}
	driver.mu.Lock()
	requests := driver.nextID
	driver.mu.Unlock()
	if requests != 0 {
		t.Fatalf("shadow made herdr I/O: request_count=%d", requests)
	}
}

func TestServeUnreachableHerdrDegradesWithoutStoppingIPCOrDispatch(t *testing.T) {
	t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
	socketPath := t.TempDir() + "/missing/herdr.sock"
	opts := serveLazyTestOptions(t, false, socketPath)
	deps := serveDefaultDependencies()
	defaultOpen := deps.openStore
	var store *Store
	var deliveryID string
	deps.openStore = func(options ServeOptions, logf serveLogFunc) (*Store, error) {
		opened, err := defaultOpen(options, logf)
		if err != nil {
			return nil, err
		}
		fail := func(err error) (*Store, error) {
			_ = opened.Close()
			return nil, err
		}
		if _, err := opened.PutReconcilerState(ReconcilerStateInput{
			OrgID: options.Org, PaneLabel: options.Target, AgentKind: "omp",
		}, time.Now().UnixMilli()); err != nil {
			return fail(err)
		}
		event, err := opened.InsertEvent(EventInsert{
			Connector: "test-ingest", EventKey: "live-event", ConversationID: "conversation-1",
			Content: "hello", MetaJSON: "{}", RawJSON: "{}",
		}, time.Now().UnixMilli())
		if err != nil {
			return fail(err)
		}
		delivery, err := opened.InsertDelivery(event.ID, options.Target, time.Now().UnixMilli())
		if err != nil {
			return fail(err)
		}
		store = opened
		deliveryID = delivery.ID
		return opened, nil
	}
	var logs serveTestRecorder
	logf := func(format string, args ...any) {
		logs.add(fmt.Sprintf(format, args...))
	}

	runtime, err := serveAssemble(context.Background(), opts, deps, logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	if err := runtime.start(context.Background()); err != nil {
		t.Fatalf("start with unreachable herdr: %v", err)
	}

	dispatcher, ok := runtime.dispatcher.(*Dispatcher)
	if !ok {
		t.Fatalf("default dispatcher = %T, want *Dispatcher", runtime.dispatcher)
	}
	reconcile := dispatcher.LastReconcileResult()
	if reconcile == nil || reconcile.Action != ReconcileUnavailable ||
		!strings.Contains(reconcile.Error, socketPath) {
		t.Fatalf("reconcile = %#v, want unavailable with socket cause", reconcile)
	}
	health := serveTestHealth(t, runtime)
	if health.Reconcile == nil || *health.Reconcile != string(ReconcileUnavailable) {
		t.Fatalf("health reconcile = %#v", health.Reconcile)
	}
	delivery, err := store.GetDelivery(deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != DeliveryPending || delivery.AttemptCount != 1 || delivery.LastError == nil ||
		!strings.Contains(*delivery.LastError, socketPath) {
		t.Fatalf("delivery after failed dispatch = %#v", delivery)
	}
	joinedLogs := strings.Join(logs.snapshot(), "\n")
	if !strings.Contains(joinedLogs, "startup reconcile: action=unavailable error=") ||
		!strings.Contains(joinedLogs, socketPath) {
		t.Fatalf("startup logs omit degraded reason: %s", joinedLogs)
	}
}

func TestServeStartupReclaimsBeforeHerdrThenServesBeforeDrain(t *testing.T) {
	recorder := &serveTestRecorder{}
	httpStarted := make(chan struct{})
	opts := serveLazyTestOptions(t, false, t.TempDir()+"/unused.sock")
	deps := serveDefaultDependencies()
	defaultOpen := deps.openStore
	var store *Store
	var deliveryID string
	deps.openStore = func(options ServeOptions, logf serveLogFunc) (*Store, error) {
		opened, err := defaultOpen(options, logf)
		if err != nil {
			return nil, err
		}
		fail := func(err error) (*Store, error) {
			_ = opened.Close()
			return nil, err
		}
		workspaceID := "w1"
		paneID := "w1:p1"
		source := "herdr:omp"
		kind := "id"
		session := "session-1"
		state, err := opened.PutReconcilerState(ReconcilerStateInput{
			OrgID: options.Org, WorkspaceID: &workspaceID, PaneID: &paneID,
			PaneLabel: options.Target, AgentKind: "omp", NativeSessionSource: &source,
			NativeSessionKind: &kind, NativeSessionValue: &session,
		}, 1)
		if err != nil {
			return fail(err)
		}
		event, err := opened.InsertEvent(EventInsert{
			Connector: "test-ingest", EventKey: "stale-event", ConversationID: "conversation-1",
			Content: "hello", MetaJSON: "{}", RawJSON: "{}",
		}, 1)
		if err != nil {
			return fail(err)
		}
		delivery, err := opened.InsertDelivery(event.ID, options.Target, 1)
		if err != nil {
			return fail(err)
		}
		generation := state.SessionGeneration
		claimed, err := opened.ClaimNext(options.Target, 2, &generation)
		if err != nil {
			return fail(err)
		}
		if claimed == nil || claimed.Delivery.ID != delivery.ID {
			return fail(errors.New("failed to create stale dispatched delivery"))
		}
		store = opened
		deliveryID = delivery.ID
		recorder.add("db")
		return opened, nil
	}
	deps.newDriver = func(context.Context, ServeOptions, serveLogFunc) (serveManagedDriver, error) {
		agent := Agent{
			Name: opts.Target, Agent: "omp", Status: "idle", PaneID: "w1:p1", WorkspaceID: "w1",
			Session: &AgentSession{Agent: "omp", Kind: "id", Source: "herdr:omp", Value: "session-1"},
		}
		return &serveOrderDriver{
			FakeDriver: &FakeDriver{Agents: []Agent{agent}}, t: t, store: store,
			deliveryID: deliveryID, recorder: recorder, httpStarted: httpStarted,
		}, nil
	}
	deps.newHTTPServer = func(http.Handler) serveHTTPServer {
		return &serveTestHTTPServer{
			recorder: recorder, label: "serve", started: httpStarted, closed: make(chan struct{}),
		}
	}
	logf := func(format string, args ...any) {
		if strings.HasPrefix(fmt.Sprintf(format, args...), "startup: reclaimed ") {
			recorder.add("reclaim")
		}
	}

	runtime, err := serveAssemble(context.Background(), opts, deps, logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	if err := runtime.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"db", "reclaim", "reconcile", "serve", "drain"}
	if got := recorder.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("startup order = %v, want %v", got, want)
	}
}

func TestServeTickGuardPreventsOverlap(t *testing.T) {
	store, err := Open(t.TempDir() + "/courier.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := NewRegistry()
	hostTools, err := NewHostTools(HostToolsOptions{Store: store, Connectors: registry})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	dispatcher := &serveTestDispatcher{
		start: func(context.Context) (ReconcileResult, error) { return ReconcileResult{}, nil },
		drain: func(context.Context) ([]DispatchOutcome, error) { return nil, nil },
		tick: func(context.Context) ([]DispatchOutcome, error) {
			calls.Add(1)
			close(entered)
			<-release
			return nil, nil
		},
	}
	runtime := &serveRuntime{hostTools: hostTools, dispatcher: dispatcher, log: func(string, ...any) {}}
	firstDone := make(chan struct{})
	go func() {
		runtime.runTick(context.Background())
		close(firstDone)
	}()
	<-entered

	var overlaps sync.WaitGroup
	for range 16 {
		overlaps.Add(1)
		go func() {
			defer overlaps.Done()
			runtime.runTick(context.Background())
		}()
	}
	overlaps.Wait()
	close(release)
	<-firstDone
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent tick calls = %d, want 1", got)
	}
}

func TestServeShadowHeartbeatAndZeroDisable(t *testing.T) {
	for _, test := range []struct {
		name     string
		interval time.Duration
		wantLog  bool
	}{
		{name: "periodic while idle", interval: time.Millisecond, wantLog: true},
		{name: "zero disables", interval: 0, wantLog: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(t.TempDir() + "/courier.sqlite")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			var heartbeats atomic.Int64
			runtime := &serveRuntime{
				opts:  ServeOptions{Target: "agent-test", Shadow: NewShadowMode(true), ShadowHeartbeat: test.interval},
				store: store,
				log: func(format string, _ ...any) {
					if strings.HasPrefix(format, "SHADOW heartbeat:") {
						heartbeats.Add(1)
					}
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			runtime.startLoops(ctx)
			if test.wantLog {
				deadline := time.Now().Add(time.Second)
				for heartbeats.Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			<-runtime.loopDone
			if got := heartbeats.Load() > 0; got != test.wantLog {
				t.Fatalf("heartbeat logged = %v, want %v", got, test.wantLog)
			}
		})
	}
}
