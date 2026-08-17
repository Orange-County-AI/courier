package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type DispatcherOptions struct {
	Store           *Store
	Driver          Driver
	Target          string
	OrgID           string
	PromptTimeout   time.Duration
	ExtraArgs       []string
	Log             func(string)
	Now             func() int64
	EnvelopePreview *bool
	DraftGuard      *bool
	NotifyHolds     *bool
	Shadow          ShadowMode
	Connectors      *Registry
}

// TargetStatus is the freshest evidence that the routing target resolves.
// Successful dispatches update it because reconcile only runs at startup and
// after failure; reporting reconcile alone false-alarmed on a healthy host.
type TargetStatus struct {
	Action ReconcileAction `json:"action"`
	At     int64           `json:"at"`
	Source string          `json:"source"`
}

// DraftHold is the pane evidence that stopped a drain: the human had unsent
// input in the composer, so dispatching would have submitted their draft along
// with the message. It is retried, never dropped.
type DraftHold struct {
	PaneID string `json:"pane_id"`
	Agent  string `json:"agent"`
	At     int64  `json:"at"`
}

type DispatchOutcome struct {
	DeliveryID string `json:"delivery_id"`
	EventID    int64  `json:"event_id"`
	OK         bool   `json:"ok"`
	Blocked    bool   `json:"blocked,omitempty"`
	Error      string `json:"error,omitempty"`
}

type Dispatcher struct {
	store         *Store
	driver        Driver
	target        string
	orgID         string
	promptTimeout time.Duration
	extraArgs     []string
	log           func(string)
	now           func() int64
	previewOn     bool
	draftGuard    bool
	notifyHolds   bool
	shadow        ShadowMode
	connectors    *Registry

	drainMu sync.Mutex
	stateMu sync.RWMutex
	last    *ReconcileResult
	status  *TargetStatus
	hold    *DraftHold
	paused  bool
}

func NewDispatcher(opts DispatcherOptions) (*Dispatcher, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("dispatcher: store is required")
	}
	if opts.Driver == nil {
		return nil, fmt.Errorf("dispatcher: driver is required")
	}
	if opts.Target == "" {
		return nil, fmt.Errorf("dispatcher: target is required")
	}
	orgID := opts.OrgID
	if orgID == "" {
		orgID = opts.Target
	}
	promptTimeout := opts.PromptTimeout
	if promptTimeout <= 0 {
		promptTimeout = DefaultPromptTimeout
	}
	log := opts.Log
	if log == nil {
		log = func(string) {}
	}
	now := opts.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	previewOn := true
	if opts.EnvelopePreview != nil {
		previewOn = *opts.EnvelopePreview
	}
	draftGuard := true
	if opts.DraftGuard != nil {
		draftGuard = *opts.DraftGuard
	}
	notifyHolds := true
	if opts.NotifyHolds != nil {
		notifyHolds = *opts.NotifyHolds
	}
	return &Dispatcher{
		store:         opts.Store,
		driver:        opts.Driver,
		target:        opts.Target,
		orgID:         orgID,
		promptTimeout: promptTimeout,
		extraArgs:     append([]string(nil), opts.ExtraArgs...),
		log:           log,
		now:           now,
		previewOn:     previewOn,
		draftGuard:    draftGuard,
		notifyHolds:   notifyHolds,
		shadow:        opts.Shadow,
		connectors:    opts.Connectors,
	}, nil
}

func (d *Dispatcher) Target() string { return d.target }

func (d *Dispatcher) OrgID() string { return d.orgID }

// Start recovers only dispatches too old to belong to another live prompt,
// backfills delivery-less events, then reconciles before any drain is allowed.
func (d *Dispatcher) Start(ctx context.Context) (ReconcileResult, error) {
	if d.shadow.Suppressed() {
		// No reclaim, backfill, or reconcile. Reconcile can start an agent: the
		// loudest possible side effect from an observation-only process.
		d.log("SHADOW: dispatcher not started — no herdr calls, nothing will be prompted")
		state, err := d.store.GetReconcilerState(d.orgID)
		if err != nil {
			return ReconcileResult{}, err
		}
		return reconcileUnavailable(state, "shadow mode"), nil
	}
	reclaimed, err := d.store.ReclaimStaleDispatches(d.promptTimeout.Milliseconds(), d.now())
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("reclaim stale dispatches: %w", err)
	}
	if len(reclaimed) > 0 {
		d.log(fmt.Sprintf("startup: reclaimed %d stale dispatch(es)", len(reclaimed)))
	}
	if _, err := d.Backfill(); err != nil {
		return ReconcileResult{}, err
	}
	return d.Reconcile(ctx)
}

// Backfill aims unhandled imports and events whose only delivery was abandoned.
// Connector ingest normally creates both rows; without this recovery those two
// cases form a queue that is correct in every column and permanently inert.
func (d *Dispatcher) Backfill() ([]string, error) {
	deliveries, err := d.store.Backfill(d.target, d.now())
	if err != nil {
		return nil, fmt.Errorf("backfill deliveries: %w", err)
	}
	ids := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		ids[i] = delivery.ID
	}
	if len(ids) > 0 {
		d.log(fmt.Sprintf("startup: backfilled %d delivery(ies) for unhandled events that had none", len(ids)))
	}
	return ids, nil
}

func (d *Dispatcher) Reconcile(ctx context.Context) (ReconcileResult, error) {
	if d.shadow.Suppressed() {
		state, err := d.store.GetReconcilerState(d.orgID)
		if err != nil {
			return ReconcileResult{}, err
		}
		return reconcileUnavailable(state, "shadow mode"), nil
	}
	result, err := Reconcile(ctx, ReconcileOptions{
		Store:     d.store,
		Driver:    d.driver,
		OrgID:     d.orgID,
		ExtraArgs: d.extraArgs,
		Log:       d.log,
		Now:       d.now,
	})
	if err != nil {
		return ReconcileResult{}, err
	}
	d.stateMu.Lock()
	d.last = dispatchCopyReconcileResult(result)
	d.status = &TargetStatus{Action: result.Action, At: d.now(), Source: "reconcile"}
	d.stateMu.Unlock()
	return result, nil
}

func (d *Dispatcher) LastReconcileResult() *ReconcileResult {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	if d.last == nil {
		return nil
	}
	return dispatchCopyReconcileResult(*d.last)
}

func (d *Dispatcher) TargetStatus() *TargetStatus {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	if d.status == nil {
		return nil
	}
	status := *d.status
	return &status
}

// DraftHold reports the composer state that is currently holding dispatch back,
// or nil when nothing is held.
func (d *Dispatcher) DraftHold() *DraftHold {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	if d.hold == nil {
		return nil
	}
	hold := *d.hold
	return &hold
}

// SetPaused stops or resumes claiming. Pause is process-lifetime state and
// deliberately not persisted: a courier restart resumes delivery, so a paused
// gateway can never become a silently permanent one.
func (d *Dispatcher) SetPaused(paused bool) {
	d.stateMu.Lock()
	d.paused = paused
	d.stateMu.Unlock()
}

func (d *Dispatcher) Paused() bool {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	return d.paused
}

// Sweep returns unconfirmed deliveries to the queue after their read-sensitive
// backoff. It never settles or abandons a delivery.
func (d *Dispatcher) Sweep(now ...int64) ([]string, error) {
	at := d.now()
	if len(now) > 0 {
		at = now[0]
	}
	reclaimed, err := d.store.SweepStuckDispatches(at, d.target)
	if err != nil {
		return nil, fmt.Errorf("sweep dispatches: %w", err)
	}
	if len(reclaimed) > 0 {
		d.log(fmt.Sprintf("sweep: returning %d unconfirmed delivery(ies) to the queue", len(reclaimed)))
	}
	return reclaimed, nil
}

// Drain dispatches claimable events serially. ClaimNext moves each row to
// dispatched before PromptAgent blocks, making one-in-flight a durable fact.
func (d *Dispatcher) Drain(ctx context.Context) ([]DispatchOutcome, error) {
	// This is before TryLock so shadow cannot claim a row or burn an attempt even
	// if another caller is currently draining.
	if d.shadow.Suppressed() {
		return nil, nil
	}
	// Same reasoning as the shadow check: a paused dispatcher must claim nothing
	// and burn no attempt, whoever else is draining.
	if d.Paused() {
		return nil, nil
	}
	if !d.drainMu.TryLock() {
		return nil, nil
	}
	defer d.drainMu.Unlock()

	outcomes := make([]DispatchOutcome, 0)
	for {
		state, err := d.store.GetReconcilerState(d.orgID)
		if err != nil {
			return outcomes, fmt.Errorf("get dispatch generation: %w", err)
		}
		var generation *int64
		if state != nil {
			value := state.SessionGeneration
			generation = &value
		}
		// The draft guard runs before ClaimNext so a held delivery keeps its
		// place in the queue: no attempt is burned, no last_error is written,
		// and the row stays pending for the next tick.
		if d.draftGuard {
			claimable, err := d.store.HasClaimable(d.target)
			if err != nil {
				return outcomes, fmt.Errorf("peek claimable delivery: %w", err)
			}
			if !claimable {
				d.releaseDraftHold()
				return outcomes, nil
			}
			if hold := d.composerHold(ctx); hold != nil {
				d.applyDraftHold(ctx, *hold)
				return outcomes, nil
			}
			d.releaseDraftHold()
		}
		claimed, err := d.store.ClaimNext(d.target, d.now(), generation)
		if err != nil {
			return outcomes, fmt.Errorf("claim next delivery: %w", err)
		}
		if claimed == nil {
			return outcomes, nil
		}

		user := ""
		if claimed.Event.User != nil {
			user = *claimed.Event.User
		}
		text := BuildEnvelope(EnvelopeInput{
			DeliveryID:     claimed.Delivery.ID,
			ConversationID: claimed.Event.ConversationID,
			User:           user,
			Connector:      claimed.Event.Connector,
			AttemptCount:   int(claimed.Delivery.AttemptCount),
			MetaJSON:       claimed.Event.MetaJSON,
			Content:        claimed.Event.Content,
			Read:           claimed.Delivery.ReadAt != nil,
			PreviewOn:      d.previewOn,
		})
		result := d.driver.PromptAgent(ctx, d.target, text, d.promptTimeout)
		if result.OK {
			// Still dispatched. Herdr consumed the prompt; only chat_reply or an
			// explicit mark_handled can establish that the message was answered.
			// Blocked is exit 0 too and is therefore also left unconfirmed.
			if err := d.store.ConfirmDispatched(claimed.Delivery.ID, d.now()); err != nil {
				return outcomes, fmt.Errorf("confirm dispatched %s: %w", claimed.Delivery.ID, err)
			}
			d.stateMu.Lock()
			d.status = &TargetStatus{Action: ReconcileRefreshed, At: d.now(), Source: "dispatch"}
			d.stateMu.Unlock()
			outcomes = append(outcomes, DispatchOutcome{
				DeliveryID: claimed.Delivery.ID,
				EventID:    claimed.Event.ID,
				OK:         true,
				Blocked:    result.Blocked,
			})
			continue
		}

		message := result.Error
		if message == "" {
			message = result.Code
		}
		if message == "" {
			message = "prompt failed"
		}
		code := result.Code
		if code == "" {
			code = "error"
		}
		if err := d.store.ReleaseToPending(claimed.Delivery.ID, code+": "+message, d.now()); err != nil {
			return outcomes, fmt.Errorf("release delivery %s: %w", claimed.Delivery.ID, err)
		}
		outcomes = append(outcomes, DispatchOutcome{
			DeliveryID: claimed.Delivery.ID,
			EventID:    claimed.Event.ID,
			Error:      message,
		})
		d.log(fmt.Sprintf("dispatch failed for %s — %s %s", claimed.Delivery.ID, result.Code, message))
		// Reconcile distinguishes an unavailable target from an ordinary prompt
		// failure. Only the former may produce a channel notice; the delivery
		// was already returned to pending and remains eligible for retry.
		reconcile, err := d.Reconcile(ctx)
		if err != nil {
			return outcomes, err
		}
		if reconcile.Action == ReconcileUnavailable {
			d.notifyUnavailable(ctx, *claimed)
		}
		return outcomes, nil
	}
}

// composerHold reads the target pane and reports a hold only on positive
// evidence of unsent human input. Resolution and read failures return nil: the
// prompt path already reports an unreachable or missing target, and a guard that
// held on its own errors would stall a durable queue.
func (d *Dispatcher) composerHold(ctx context.Context) *DraftHold {
	agent, err := d.driver.GetAgent(ctx, d.target)
	if err != nil || agent == nil || agent.PaneID == "" {
		return nil
	}
	screen, err := d.driver.PaneScreen(ctx, agent.PaneID)
	if err != nil {
		return nil
	}
	if DetectComposer(agent.Agent, screen) != ComposerDraft {
		return nil
	}
	return &DraftHold{PaneID: agent.PaneID, Agent: agent.Agent, At: d.now()}
}

func (d *Dispatcher) applyDraftHold(ctx context.Context, hold DraftHold) {
	d.stateMu.Lock()
	first := d.hold == nil || d.hold.PaneID != hold.PaneID
	d.hold = &hold
	d.stateMu.Unlock()
	if !first {
		return
	}
	d.log(fmt.Sprintf("dispatch held — %s has unsent input in pane %s; retrying until the composer is clear", hold.Agent, hold.PaneID))
	if !d.notifyHolds {
		return
	}
	// One toast on the same edge the log line is written. A held message is
	// otherwise invisible, and re-notifying every tick would train the human to
	// ignore it. A notification is an extra: its failure never fails the drain.
	if err := d.driver.Notify(ctx, "courier: message waiting", d.holdNotice()); err != nil {
		d.log(fmt.Sprintf("notify failed — %v", err))
	}
}

// holdNotice names who is waiting and how many, so the toast is actionable
// without opening the inbox. A listing failure degrades to the bare reason
// rather than suppressing the notification.
func (d *Dispatcher) holdNotice() string {
	open, err := d.store.OpenDeliveries(d.target)
	if err != nil || len(open) == 0 {
		return "delivery held until your composer is clear"
	}
	who := open[0].Event.Connector
	if user := open[0].Event.User; user != nil && *user != "" {
		who = *user + " on " + who
	}
	return fmt.Sprintf("%s — %d waiting until your composer is clear", who, len(open))
}

func (d *Dispatcher) releaseDraftHold() {
	d.stateMu.Lock()
	released := d.hold
	d.hold = nil
	d.stateMu.Unlock()
	if released != nil {
		d.log(fmt.Sprintf("dispatch resumed — pane %s composer is clear", released.PaneID))
	}
}

func (d *Dispatcher) notifyUnavailable(ctx context.Context, claimed Deliverable) {
	if d.connectors == nil {
		return
	}
	connector := d.connectors.Get(claimed.Event.Connector)
	notifier, ok := connector.(unavailableNotifier)
	if !ok {
		return
	}
	err := notifier.NotifyUnavailable(ctx, DeliveryContext{
		Delivery: claimed.Delivery, Event: claimed.Event, ConversationID: claimed.Event.ConversationID,
	})
	if err != nil {
		d.log(fmt.Sprintf("unavailable notice failed for %s — %v", claimed.Delivery.ID, err))
	}
}

func (d *Dispatcher) Tick(ctx context.Context) ([]DispatchOutcome, error) {
	status := d.TargetStatus()
	if !d.shadow.Suppressed() && status != nil && status.Action == ReconcileUnavailable {
		if _, err := d.Reconcile(ctx); err != nil {
			return nil, err
		}
	}
	if _, err := d.Sweep(); err != nil {
		return nil, err
	}
	return d.Drain(ctx)
}

func dispatchCopyReconcileResult(result ReconcileResult) *ReconcileResult {
	copy := result
	return &copy
}
