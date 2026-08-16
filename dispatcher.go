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
	shadow        ShadowMode
	connectors    *Registry

	drainMu sync.Mutex
	stateMu sync.RWMutex
	last    *ReconcileResult
	status  *TargetStatus
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
