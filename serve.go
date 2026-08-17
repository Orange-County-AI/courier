package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const serveShutdownTimeout = 30 * time.Second

type serveLogFunc func(string, ...any)

type serveManagedDriver interface {
	Driver
	Close() error
}

type serveDispatcher interface {
	Start(context.Context) (ReconcileResult, error)
	Drain(context.Context) ([]DispatchOutcome, error)
	Tick(context.Context) ([]DispatchOutcome, error)
	TargetStatus() *TargetStatus
	DraftHold() *DraftHold
	Paused() bool
	SetPaused(bool)
}

type serveHTTPServer interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
}

type serveConnectorCandidate struct {
	name    string
	enabled bool
	build   func() (Connector, error)
}

type serveDependencies struct {
	openStore       func(ServeOptions, serveLogFunc) (*Store, error)
	closeStore      func(*Store) error
	newDriver       func(context.Context, ServeOptions, serveLogFunc) (serveManagedDriver, error)
	buildConnectors func(*Store, ServeOptions, serveLogFunc) (*Registry, []Connector, error)
	newDispatcher   func(*Store, serveManagedDriver, *Registry, ServeOptions, serveLogFunc) (serveDispatcher, error)
	listenIPC       func(IPCListenOptions) (net.Listener, error)
	newHTTPServer   func(http.Handler) serveHTTPServer
}

type serveRuntime struct {
	opts       ServeOptions
	deps       serveDependencies
	log        serveLogFunc
	store      *Store
	driver     serveManagedDriver
	registry   *Registry
	connectors []Connector
	started    []Connector
	dispatcher serveDispatcher
	hostTools  *HostTools
	manifest   Manifest
	handler    http.Handler
	listener   net.Listener
	server     serveHTTPServer
	serveErr   chan error
	loopCancel context.CancelFunc
	loopDone   chan struct{}
	tickMu     sync.Mutex
}

func serveDefaultDependencies() serveDependencies {
	return serveDependencies{
		openStore: func(opts ServeOptions, logf serveLogFunc) (*Store, error) {
			return Open(
				opts.DBPath,
				WithRedeliverGrace(opts.RedeliverGrace.Milliseconds()),
				WithRedeliverMaxBackoff(opts.RedeliverMaxBackoff.Milliseconds()),
				WithRedeliverReadFactor(int64(opts.RedeliverReadFactor)),
				WithMigrationLogger(func(message string) { logf("%s", message) }),
			)
		},
		closeStore: func(store *Store) error { return store.Close() },
		newDriver: func(ctx context.Context, opts ServeOptions, logf serveLogFunc) (serveManagedDriver, error) {
			return NewSocketDriver(ctx, SocketDriverOptions{
				SocketPath: opts.Herdr.SocketPath,
				Session:    opts.Herdr.Session,
				Log:        func(message string) { logf("%s", message) },
			})
		},
		buildConnectors: serveBuildConnectors,
		newDispatcher: func(store *Store, driver serveManagedDriver, connectors *Registry, opts ServeOptions, logf serveLogFunc) (serveDispatcher, error) {
			preview := opts.EnvelopePreview
			draftGuard := opts.DraftGuard
			notifyHolds := opts.DraftNotify
			return NewDispatcher(DispatcherOptions{
				Store:           store,
				Driver:          driver,
				Target:          opts.Target,
				OrgID:           opts.Org,
				PromptTimeout:   opts.PromptTimeout,
				EnvelopePreview: &preview,
				DraftGuard:      &draftGuard,
				NotifyHolds:     &notifyHolds,
				Shadow:          opts.Shadow,
				Connectors:      connectors,
				Log:             func(message string) { logf("%s", message) },
			})
		},
		listenIPC: ListenIPC,
		newHTTPServer: func(handler http.Handler) serveHTTPServer {
			return &http.Server{Handler: handler}
		},
	}
}

func serveBuildConnectors(store *Store, opts ServeOptions, logf serveLogFunc) (*Registry, []Connector, error) {
	candidates := []serveConnectorCandidate{
		{
			name:    MattermostName,
			enabled: opts.Mattermost.Enabled,
			build: func() (Connector, error) {
				return NewMattermostConnector(MattermostConnectorConfig{
					Store: store, Target: opts.Target, Options: opts.Mattermost, Shadow: opts.Shadow, Log: logf,
				})
			},
		},
		{
			name:    GmailName,
			enabled: opts.Gmail.Enabled,
			build: func() (Connector, error) {
				return NewGmailConnectorFromOptions(store, opts.Target, opts.Gmail, opts.Shadow)
			},
		},
		{
			name:    TelegramName,
			enabled: opts.Telegram.Enabled,
			build: func() (Connector, error) {
				return NewTelegramConnector(TelegramConnectorConfig{
					Store: store, Target: opts.Target, Options: opts.Telegram, Shadow: opts.Shadow, Log: logf,
				})
			},
		},
		{
			name:    KaneoName,
			enabled: opts.Kaneo.Enabled,
			build: func() (Connector, error) {
				return NewKaneoConnector(KaneoConnectorConfig{
					Store: store, Target: opts.Target, Options: opts.Kaneo, Shadow: opts.Shadow, Log: logf,
				})
			},
		},
	}
	return serveActivateConnectors(opts.Connectors, candidates, logf)
}

// serveActivateConnectors makes an explicit connector allowlist authoritative.
// A named connector still has to satisfy its own credential/config presence
// gate; otherwise courier says exactly what stayed inactive instead of silently
// starting a partial host.
func serveActivateConnectors(allowlist []string, candidates []serveConnectorCandidate, logf serveLogFunc) (*Registry, []Connector, error) {
	registry := NewRegistry()
	allowed := make(map[string]struct{}, len(allowlist))
	for _, name := range allowlist {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			allowed[name] = struct{}{}
		}
	}
	known := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		known[candidate.name] = struct{}{}
	}
	if len(allowed) > 0 {
		for name := range allowed {
			if _, ok := known[name]; !ok {
				logf("WARNING: connector %q was requested but is unknown; it stays inactive", name)
			}
		}
	}

	active := make([]Connector, 0, len(candidates))
	for _, candidate := range candidates {
		if len(allowed) > 0 {
			if _, ok := allowed[candidate.name]; !ok {
				continue
			}
		}
		if !candidate.enabled {
			if _, requested := allowed[candidate.name]; requested {
				logf("WARNING: connector %s was requested but its required configuration is absent; it stays inactive", candidate.name)
			}
			continue
		}
		connector, err := candidate.build()
		if err != nil {
			return nil, nil, fmt.Errorf("configure %s connector: %w", candidate.name, err)
		}
		if connector == nil {
			return nil, nil, fmt.Errorf("configure %s connector: constructor returned nil", candidate.name)
		}
		if err := registry.Register(connector); err != nil {
			return nil, nil, err
		}
		active = append(active, connector)
		logf("connector active: %s", candidate.name)
	}
	return registry, active, nil
}

func serveCallTool(ctx context.Context, hostTools *HostTools, connectors []Connector, name string, args map[string]any) (ToolResult, error) {
	for _, hostName := range HostToolNames {
		if name == hostName {
			return hostTools.CallTool(ctx, name, args)
		}
	}
	for _, connector := range connectors {
		for _, tool := range connector.ManifestTools() {
			if tool.Name == name {
				return connector.CallTool(ctx, name, args)
			}
		}
	}
	return ToolResult{}, fmt.Errorf("unknown tool: %s", name)
}

func serveHealthState(opts ServeOptions, connectors []Connector, dispatcher serveDispatcher) HealthState {
	names := make([]string, len(connectors))
	for i, connector := range connectors {
		names[i] = connector.Name()
	}
	state := HealthState{
		Org:        opts.Org,
		Target:     opts.Target,
		Shadow:     opts.Shadow.Enabled,
		Paused:     dispatcher.Paused(),
		Connectors: names,
		HostTools:  append([]string(nil), HostToolNames...),
	}
	if status := dispatcher.TargetStatus(); status != nil {
		action := string(status.Action)
		at := status.At
		source := status.Source
		state.Reconcile = &action
		state.ReconcileAt = &at
		state.ReconcileSource = &source
	}
	if hold := dispatcher.DraftHold(); hold != nil {
		pane := hold.PaneID
		at := hold.At
		state.DraftHoldPane = &pane
		state.DraftHoldAt = &at
	}
	return state
}

// serveInboxState renders the open queue for the plugin pane, including the
// reason delivery is held. The preview is the same 100-char pointer the envelope
// carries, so the pane and the agent describe a message identically.
func serveInboxState(store *Store, opts ServeOptions, dispatcher serveDispatcher) (InboxState, error) {
	open, err := store.OpenDeliveries(opts.Target)
	if err != nil {
		return InboxState{}, fmt.Errorf("list open deliveries: %w", err)
	}
	rows := make([]InboxRow, 0, len(open))
	for _, item := range open {
		user := ""
		if item.Event.User != nil {
			user = *item.Event.User
		}
		rows = append(rows, InboxRow{
			DeliveryID:     item.Delivery.ID,
			EventID:        item.Event.ID,
			Status:         string(item.Delivery.Status),
			Connector:      item.Event.Connector,
			ConversationID: item.Event.ConversationID,
			User:           user,
			AttemptCount:   int(item.Delivery.AttemptCount),
			Read:           item.Delivery.ReadAt != nil,
			CreatedAt:      item.Delivery.CreatedAt,
			LastError:      item.Delivery.LastError,
			Preview:        preview(item.Event.Content),
		})
	}
	return InboxState{
		Target:    opts.Target,
		Paused:    dispatcher.Paused(),
		DraftHold: dispatcher.DraftHold(),
		Rows:      rows,
	}, nil
}

func serveAssemble(ctx context.Context, opts ServeOptions, deps serveDependencies, logf serveLogFunc) (*serveRuntime, error) {
	runtime := &serveRuntime{opts: opts, deps: deps, log: logf}
	store, err := deps.openStore(opts, logf)
	if err != nil {
		return nil, err
	}
	runtime.store = store
	fail := func(err error) (*serveRuntime, error) {
		if runtime.driver != nil {
			_ = runtime.driver.Close()
		}
		_ = deps.closeStore(store)
		return nil, err
	}

	driver, err := deps.newDriver(ctx, opts, logf)
	if err != nil {
		return fail(err)
	}
	runtime.driver = driver
	registry, connectors, err := deps.buildConnectors(store, opts, logf)
	if err != nil {
		return fail(err)
	}
	runtime.registry = registry
	runtime.connectors = connectors
	hostTools, err := NewHostTools(HostToolsOptions{
		Store: store, Connectors: registry, Shadow: opts.Shadow,
		Log: func(values ...any) { logf("%s", fmt.Sprint(values...)) },
	})
	if err != nil {
		return fail(err)
	}
	runtime.hostTools = hostTools
	dispatcher, err := deps.newDispatcher(store, driver, registry, opts, logf)
	if err != nil {
		return fail(err)
	}
	runtime.dispatcher = dispatcher
	manifest, err := BuildManifest(BuildManifestOptions{
		Name:       "courier-" + opts.Org,
		Version:    buildVersion(),
		Connectors: connectors,
	})
	if err != nil {
		return fail(err)
	}
	runtime.manifest = manifest
	handler, err := NewIPCHandler(IPCOptions{
		Store: store,
		Manifest: func() (Manifest, error) {
			return manifest, nil
		},
		CallTool: func(callCtx context.Context, name string, args map[string]any) (ToolResult, error) {
			return serveCallTool(callCtx, hostTools, connectors, name, args)
		},
		Health: func() HealthState {
			return serveHealthState(opts, connectors, dispatcher)
		},
		Inbox: func(context.Context) (InboxState, error) {
			return serveInboxState(store, opts, dispatcher)
		},
		// A kick goes through runTick, not Drain: Drain alone skips reply retry
		// and reconcile, and would run beside the periodic tick instead of
		// serialized with it.
		Kick: func(kickCtx context.Context) (KickResult, error) {
			outcomes, busy := runtime.runTick(kickCtx)
			return KickResult{Busy: busy, Outcomes: outcomes}, nil
		},
		Pause: func(pauseCtx context.Context, paused bool) (bool, error) {
			dispatcher.SetPaused(paused)
			if paused {
				logf("delivery paused by request — nothing will be claimed until resumed")
				return dispatcher.Paused(), nil
			}
			// Resume delivers now rather than at the next tick: a human who just
			// pressed resume is watching the pane.
			logf("delivery resumed by request")
			runtime.runTick(pauseCtx)
			return dispatcher.Paused(), nil
		},
		Log: func(values ...any) { logf("%s", fmt.Sprint(values...)) },
	})
	if err != nil {
		return fail(err)
	}
	runtime.handler = handler
	return runtime, nil
}

func (r *serveRuntime) start(ctx context.Context) error {
	reconcile, err := r.dispatcher.Start(ctx)
	if err != nil {
		return err
	}
	r.log("startup reconcile: action=%s error=%s", reconcile.Action, reconcile.Error)

	for _, connector := range r.connectors {
		if err := connector.Start(ctx); err != nil {
			r.log("connector %s failed to start and stays inactive: %v", connector.Name(), err)
			continue
		}
		r.started = append(r.started, connector)
	}

	listener, err := r.deps.listenIPC(IPCListenOptions{Bind: r.opts.Bind, Port: r.opts.Port, Socket: r.opts.Socket})
	if err != nil {
		return err
	}
	r.listener = listener
	r.server = r.deps.newHTTPServer(r.handler)
	r.serveErr = make(chan error, 1)
	go func() {
		r.serveErr <- r.server.Serve(listener)
	}()
	if r.opts.Socket != "" {
		r.log("IPC listening on unix:%s", r.opts.Socket)
	} else {
		r.log("IPC listening on http://%s", listener.Addr())
	}

	// The listener is bound before the first drain. A newly delivered prompt can
	// therefore call read_message/chat_reply immediately, including while this
	// startup drain is still in flight.
	if _, err := r.dispatcher.Drain(ctx); err != nil && ctx.Err() == nil {
		r.log("initial drain failed: %v", err)
	}
	r.startLoops(ctx)
	return nil
}

func (r *serveRuntime) startLoops(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	r.loopCancel = cancel
	r.loopDone = make(chan struct{})
	go func() {
		defer close(r.loopDone)
		var tick *time.Ticker
		var tickC <-chan time.Time
		if !r.opts.Shadow.Enabled && r.opts.TickInterval > 0 {
			tick = time.NewTicker(r.opts.TickInterval)
			tickC = tick.C
			defer tick.Stop()
		}
		var heartbeat *time.Ticker
		var heartbeatC <-chan time.Time
		if r.opts.Shadow.Enabled && r.opts.ShadowHeartbeat > 0 {
			heartbeat = time.NewTicker(r.opts.ShadowHeartbeat)
			heartbeatC = heartbeat.C
			defer heartbeat.Stop()
		}
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-tickC:
				r.runTick(loopCtx)
			case <-heartbeatC:
				r.logShadowHeartbeat()
			}
		}
	}()
}

// runTick reports how many deliveries it dispatched and whether another tick
// already held the lock. The periodic loop ignores both; an on-demand kick needs
// them to tell a human "delivered 2" from "busy".
func (r *serveRuntime) runTick(ctx context.Context) (int, bool) {
	if !r.tickMu.TryLock() {
		return 0, true
	}
	defer r.tickMu.Unlock()
	if _, err := r.hostTools.RetryPosts(ctx); err != nil {
		r.log("post retry sweep failed: %v", err)
	}
	outcomes, err := r.dispatcher.Tick(ctx)
	if err != nil {
		r.log("dispatcher tick failed: %v", err)
	}
	return len(outcomes), false
}

func (r *serveRuntime) logShadowHeartbeat() {
	events, eventsErr := r.store.CountEvents()
	stats, statsErr := r.store.DeliveryStats(r.opts.Target, time.Now().UnixMilli())
	unposted, postsErr := r.store.UnpostedReplies()
	if err := errors.Join(eventsErr, statsErr, postsErr); err != nil {
		r.log("SHADOW heartbeat failed: %v", err)
		return
	}
	r.log(
		"SHADOW heartbeat: events=%d unread=%d read_unconfirmed=%d unposted_replies=%d connectors=%d",
		events, stats.Unread, stats.ReadUnconfirmed, len(unposted), len(r.connectors),
	)
}

func (r *serveRuntime) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.serveErr:
		if err == nil {
			return errors.New("IPC server stopped unexpectedly")
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("IPC server: %w", err)
	}
}

func (r *serveRuntime) shutdown() error {
	r.log("shutting down")
	if r.loopCancel != nil {
		r.loopCancel()
		<-r.loopDone
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), serveShutdownTimeout)
	defer cancel()
	var shutdownErrors []error
	for i := len(r.started) - 1; i >= 0; i-- {
		connector := r.started[i]
		if err := connector.Stop(shutdownCtx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("stop %s connector: %w", connector.Name(), err))
		}
	}
	if r.server != nil {
		if err := r.server.Shutdown(shutdownCtx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("shutdown IPC: %w", err))
		}
	}
	if r.listener != nil {
		if err := r.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("close IPC listener: %w", err))
		}
	}
	if r.driver != nil {
		if err := r.driver.Close(); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("close herdr driver: %w", err))
		}
	}
	if r.store != nil {
		if err := r.deps.closeStore(r.store); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("close store: %w", err))
		}
	}
	return errors.Join(shutdownErrors...)
}

func serveWithDependencies(ctx context.Context, opts ServeOptions, deps serveDependencies, logf serveLogFunc) error {
	runtime, err := serveAssemble(ctx, opts, deps, logf)
	if err != nil {
		return err
	}
	runErr := runtime.start(ctx)
	if runErr == nil {
		runErr = runtime.wait(ctx)
	}
	shutdownErr := runtime.shutdown()
	return errors.Join(runErr, shutdownErr)
}

func serveMain(opts ServeOptions) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[courier] "+format+"\n", args...)
	}
	return serveWithDependencies(ctx, opts, serveDefaultDependencies(), logf)
}
