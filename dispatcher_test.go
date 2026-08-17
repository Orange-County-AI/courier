package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDispatchSuccessStaysUnsettledAndRedelivers(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryDispatched {
		t.Fatalf("status = %q, want dispatched", delivery.Status)
	}
	if event := getTestEvent(t, h.Store, row.EventID); event.HandledAt != nil {
		t.Fatal("successful prompt handled the event")
	}

	h.Clock.Add(99)
	if reclaimed, err := h.Dispatcher.Sweep(); err != nil || len(reclaimed) != 0 {
		t.Fatalf("early Sweep = %v, %v", reclaimed, err)
	}
	h.Clock.Add(2)
	if reclaimed, err := h.Dispatcher.Sweep(); err != nil || !reflect.DeepEqual(reclaimed, []string{row.DeliveryID}) {
		t.Fatalf("due Sweep = %v, %v", reclaimed, err)
	}
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	prompts := h.Driver.PromptLog()
	if len(prompts) != 2 {
		t.Fatalf("prompts = %d, want 2", len(prompts))
	}
	if !strings.Contains(prompts[0].Text, `redelivery="0"`) || !strings.Contains(prompts[1].Text, `redelivery="1"`) ||
		!strings.Contains(prompts[1].Text, "delivered to you 1 time(s) before") ||
		!strings.Contains(prompts[1].Text, `mark_handled with delivery_id="`+row.DeliveryID+`"`) {
		t.Fatalf("redelivery envelope missing contract: %q", prompts[1].Text)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2", delivery.AttemptCount)
	}
	if event := getTestEvent(t, h.Store, row.EventID); event.HandledAt != nil {
		t.Fatal("redelivery handled the event")
	}
}

func TestDispatchBlockedIsConsumedButUnsettled(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "need permission"})
	h.Driver.PromptResults = []PromptResult{{OK: true, Blocked: true}}

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK || !outcomes[0].Blocked {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryDispatched {
		t.Fatalf("status = %q, want dispatched", delivery.Status)
	}
	if getTestEvent(t, h.Store, row.EventID).HandledAt != nil {
		t.Fatal("blocked prompt handled the event")
	}
	h.Clock.Add(101)
	if reclaimed, err := h.Dispatcher.Sweep(); err != nil || !reflect.DeepEqual(reclaimed, []string{row.DeliveryID}) {
		t.Fatalf("Sweep = %v, %v", reclaimed, err)
	}
}

func TestDispatchFailureReconcilesThenStops(t *testing.T) {
	for _, code := range []string{"timeout", "agent_not_found", "agent_not_ready"} {
		t.Run(code, func(t *testing.T) {
			h := dispatchNewHarness(t)
			first := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "first"})
			h.Enqueue(t, DispatchEnqueueInput{Key: "e2", Content: "second"})
			h.Driver.PromptResults = []PromptResult{{Code: code, Error: "herdr said " + code}}

			outcomes, err := h.Dispatcher.Drain(context.Background())
			if err != nil || len(outcomes) != 1 || outcomes[0].OK || outcomes[0].Error != "herdr said "+code {
				t.Fatalf("Drain = %#v, %v", outcomes, err)
			}
			delivery := getTestDelivery(t, h.Store, first.DeliveryID)
			if delivery.Status != DeliveryPending || delivery.AttemptCount != 1 || delivery.LastError == nil || !strings.Contains(*delivery.LastError, code) {
				t.Fatalf("failed delivery = %#v", delivery)
			}
			if getTestEvent(t, h.Store, first.EventID).HandledAt != nil {
				t.Fatal("failed prompt handled the event")
			}
			// The draft guard resolves the target and reads its pane before it
			// claims, so a dispatch is preceded by exactly one of each.
			wantCalls := []string{"getAgent:" + h.Target, "paneScreen:w1:p1", "prompt:" + h.Target, "getAgent:" + h.Target}
			if calls := h.Driver.CallLog(); !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf("calls = %v, want %v", calls, wantCalls)
			}
			if prompts := h.Driver.PromptLog(); len(prompts) != 1 {
				t.Fatalf("prompt count = %d, want 1", len(prompts))
			}
			if notices := h.Connector.UnavailableLog(); len(notices) != 0 {
				t.Fatalf("ordinary prompt failure sent unavailable notice: %#v", notices)
			}
		})
	}
}

func TestDispatchFailureRestartsAbsentAgentWithSession(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hi"})
	h.Driver.PromptResults = []PromptResult{{Code: "agent_not_found", Error: "no such agent"}}
	h.Driver.Agents = nil

	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	starts := h.Driver.StartLog()
	if len(starts) != 1 || starts[0].Name != h.Target || starts[0].Kind != "claude" || starts[0].PaneID != "w1:p1" ||
		!reflect.DeepEqual(starts[0].ExtraArgs, []string{"--resume=sess-1"}) {
		t.Fatalf("starts = %#v", starts)
	}
	state, err := h.Store.GetReconcilerState(h.OrgID)
	if err != nil || state == nil || state.SessionGeneration != 1 {
		t.Fatalf("state = %#v, %v", state, err)
	}
}

func TestReconcileUnreachableDoesNotStartAgent(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.GetAgentErr = errors.New("connect ENOENT /run/herdr.sock")

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileUnavailable {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if starts := h.Driver.StartLog(); len(starts) != 0 {
		t.Fatalf("started on socket failure: %#v", starts)
	}
	state, err := h.Store.GetReconcilerState(h.OrgID)
	if err != nil || state.SessionGeneration != 0 {
		t.Fatalf("state = %#v, %v", state, err)
	}
}

func TestTickRetriesUnavailableStartupReconcile(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.GetAgentErr = errors.New("herdr is still restoring")
	if result, err := h.Dispatcher.Reconcile(context.Background()); err != nil || result.Action != ReconcileUnavailable {
		t.Fatalf("initial Reconcile = %#v, %v", result, err)
	}

	h.Driver.GetAgentErr = nil
	if outcomes, err := h.Dispatcher.Tick(context.Background()); err != nil || len(outcomes) != 0 {
		t.Fatalf("Tick = %#v, %v", outcomes, err)
	}
	status := h.Dispatcher.TargetStatus()
	if status == nil || status.Action != ReconcileRefreshed || status.Source != "reconcile" {
		t.Fatalf("target status = %#v", status)
	}
	if calls := h.Driver.CallLog(); !reflect.DeepEqual(calls, []string{"getAgent:" + h.Target, "getAgent:" + h.Target}) {
		t.Fatalf("calls = %v", calls)
	}
}

func TestDispatchOrdersByEventAndSerializesPrompts(t *testing.T) {
	h := dispatchNewHarness(t)
	first := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "first message"})
	second := h.Enqueue(t, DispatchEnqueueInput{Key: "e2", Content: "second message"})

	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	prompts := h.Driver.PromptLog()
	if h.Driver.MaxPromptConcurrency() != 1 || len(prompts) != 2 {
		t.Fatalf("concurrency/prompts = %d/%d", h.Driver.MaxPromptConcurrency(), len(prompts))
	}
	if !strings.Contains(prompts[0].Text, `delivery_id="`+first.DeliveryID+`"`) ||
		!strings.Contains(prompts[1].Text, `delivery_id="`+second.DeliveryID+`"`) {
		t.Fatalf("prompt order = %#v", prompts)
	}
}

func TestConcurrentDrainIsNoOpWhileHeld(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "only"})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h.Driver.PromptStarted = started
	h.Driver.PromptRelease = release

	firstDone := make(chan error, 1)
	go func() {
		_, err := h.Dispatcher.Drain(context.Background())
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first drain did not reach prompt")
	}
	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || outcomes != nil {
		t.Fatalf("overlapping Drain = %#v, %v", outcomes, err)
	}
	if prompts := h.Driver.PromptLog(); len(prompts) != 1 {
		t.Fatalf("prompts while first drain held = %d, want 1", len(prompts))
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if h.Driver.MaxPromptConcurrency() != 1 {
		t.Fatalf("max prompt concurrency = %d", h.Driver.MaxPromptConcurrency())
	}
}

func TestDispatchOrdersReclaimedDeliveryByEventID(t *testing.T) {
	h := dispatchNewHarness(t)
	first := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "first"})
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.Clock.Add(101)
	second := h.Enqueue(t, DispatchEnqueueInput{Key: "e2", Content: "second"})
	if _, err := h.Dispatcher.Sweep(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	prompts := h.Driver.PromptLog()[1:]
	if len(prompts) != 2 || !strings.Contains(prompts[0].Text, first.DeliveryID) || !strings.Contains(prompts[1].Text, second.DeliveryID) {
		t.Fatalf("reclaimed order = %#v", prompts)
	}
}

func TestDispatcherStartReclaimsOnlyStalePrompt(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hi"})
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryDispatched {
		t.Fatalf("young dispatch status = %q", delivery.Status)
	}
	h.Clock.Add(5_001)
	if _, err := h.Dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryPending {
		t.Fatalf("stale dispatch status = %q", delivery.Status)
	}
}

func TestDispatcherStartBackfillsDeliverylessEvent(t *testing.T) {
	h := dispatchNewHarness(t)
	event := insertTestEvent(t, h.Store, "imported", "conv", h.Clock.Now())
	if open, err := h.Store.OpenDeliveryForEvent(event.ID); err != nil || open != nil {
		t.Fatalf("precondition delivery = %#v, %v", open, err)
	}
	if _, err := h.Dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivery, err := h.Store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil || delivery.Status != DeliveryPending {
		t.Fatalf("backfilled delivery = %#v, %v", delivery, err)
	}
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if getTestDelivery(t, h.Store, delivery.ID).Status != DeliveryDispatched {
		t.Fatal("backfilled delivery was not dispatchable")
	}
}

func TestDispatcherBackfillSkipsHandledAndReaimsFailed(t *testing.T) {
	t.Run("handled", func(t *testing.T) {
		h := dispatchNewHarness(t)
		event := insertTestEvent(t, h.Store, "handled", "conv", h.Clock.Now())
		if !h.Store.MarkHandled(MarkHandledArgs{EventID: &event.ID}, h.Clock.Now()) {
			t.Fatal("MarkHandled failed")
		}
		if _, err := h.Dispatcher.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if delivery, err := h.Store.OpenDeliveryForEvent(event.ID); err != nil || delivery != nil {
			t.Fatalf("handled event delivery = %#v, %v", delivery, err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		h := dispatchNewHarness(t)
		row := h.Enqueue(t, DispatchEnqueueInput{Key: "failed", Content: "hi"})
		if err := h.Store.FailDelivery(row.DeliveryID, "abandoned"); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Dispatcher.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		fresh, err := h.Store.OpenDeliveryForEvent(row.EventID)
		if err != nil || fresh == nil || fresh.ID == row.DeliveryID || fresh.Status != DeliveryPending {
			t.Fatalf("fresh delivery = %#v, %v", fresh, err)
		}
		ids, err := h.Dispatcher.Backfill()
		if err != nil || len(ids) != 0 {
			t.Fatalf("idempotent Backfill = %v, %v", ids, err)
		}
	})
}

func TestShadowDispatcherTouchesNeitherQueueNorHerdr(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hi"})
	preview := true
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Store: h.Store, Driver: h.Driver, Target: h.Target, OrgID: h.OrgID,
		Now: h.Clock.Now, EnvelopePreview: &preview, Shadow: NewShadowMode(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := dispatcher.Start(context.Background()); err != nil || result.Action != ReconcileUnavailable {
		t.Fatalf("Start = %#v, %v", result, err)
	}
	if result, err := dispatcher.Reconcile(context.Background()); err != nil || result.Action != ReconcileUnavailable {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if outcomes, err := dispatcher.Drain(context.Background()); err != nil || outcomes != nil {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if calls := h.Driver.CallLog(); len(calls) != 0 {
		t.Fatalf("shadow herdr calls = %v", calls)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryPending || delivery.AttemptCount != 0 {
		t.Fatalf("shadow changed queue: %#v", delivery)
	}
}

func TestSuccessfulDispatchRefreshesTargetStatus(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = nil
	h.Driver.StartErr = errors.New("pane w1:p1 is already running an agent")
	if result, err := h.Dispatcher.Reconcile(context.Background()); err != nil || result.Action != ReconcileUnavailable {
		t.Fatalf("initial Reconcile = %#v, %v", result, err)
	}
	h.Driver.StartErr = nil
	h.Driver.Agents = []Agent{dispatchRestoredAgent(Agent{Name: h.Target})}
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello"})
	h.Clock.Add(60_000)

	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := h.Dispatcher.TargetStatus()
	if status == nil || status.Action != ReconcileRefreshed || status.Source != "dispatch" || status.At != h.Clock.Now() {
		t.Fatalf("status = %#v", status)
	}
	getCalls := 0
	for _, call := range h.Driver.CallLog() {
		if strings.HasPrefix(call, "getAgent:") {
			getCalls++
		}
	}
	// Startup reconcile plus the draft guard's pane resolution. A third call
	// would mean the successful dispatch re-reconciled the target.
	if getCalls != 2 {
		t.Fatalf("GetAgent calls = %d, want startup reconcile and draft guard only", getCalls)
	}
}

func TestReconcileSetsTargetStatusAndFailureSupersedesSuccess(t *testing.T) {
	h := dispatchNewHarness(t)
	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status := h.Dispatcher.TargetStatus(); status == nil || status.Action != result.Action || status.Source != "reconcile" {
		t.Fatalf("reconcile status = %#v", status)
	}

	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hi"})
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.Enqueue(t, DispatchEnqueueInput{Key: "e2", Content: "again"})
	h.Driver.PromptResults = []PromptResult{{Code: "agent_not_found", Error: "gone"}}
	h.Driver.Agents = nil
	h.Driver.StartErr = errors.New("no pane")
	if _, err := h.Dispatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := h.Dispatcher.TargetStatus(); status == nil || status.Action != ReconcileUnavailable || status.Source != "reconcile" {
		t.Fatalf("post-failure status = %#v", status)
	}
}

func TestToolCallDuringDrainDoesNotSettle(t *testing.T) {
	h := dispatchNewHarness(t)
	tools, err := NewHostTools(HostToolsOptions{
		Store: h.Store, Connectors: h.Registry, Now: h.Clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hi"})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h.Driver.PromptStarted = started
	h.Driver.PromptRelease = release

	var drainErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, drainErr = h.Dispatcher.Drain(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("drain did not reach prompt")
	}
	// Exercise the complete read_message tool while ClaimNext's dispatch is
	// live. SQLite's single connection must serialize the tool and dispatcher.
	result, err := tools.ReadMessage(context.Background(), map[string]any{
		"agent": h.Target, "delivery_id": row.DeliveryID,
	})
	if err != nil || result.IsError || !strings.Contains(result.Text, `read="first"`) {
		t.Fatalf("concurrent ReadMessage = %#v, %v", result, err)
	}
	close(release)
	wg.Wait()
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryDispatched || delivery.ReadAt == nil {
		t.Fatalf("delivery = %#v", delivery)
	}
	if event := getTestEvent(t, h.Store, row.EventID); event.HandledAt != nil {
		t.Fatal("read during drain settled the event")
	}
}

func TestDraftGuardHoldsDispatchUntilComposerClears(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 0 {
		t.Fatalf("held Drain = %#v, %v", outcomes, err)
	}
	if prompts := h.Driver.PromptLog(); len(prompts) != 0 {
		t.Fatalf("prompted into a draft: %#v", prompts)
	}
	// A hold is not a failed attempt: the row keeps its place, its attempt
	// count, and a clean last_error, so the envelope is not marked redelivered.
	delivery := getTestDelivery(t, h.Store, row.DeliveryID)
	if delivery.Status != DeliveryPending || delivery.AttemptCount != 0 || delivery.LastError != nil || delivery.LastDispatchedAt != nil {
		t.Fatalf("held delivery = %#v", delivery)
	}
	hold := h.Dispatcher.DraftHold()
	if hold == nil || hold.PaneID != "w1:p1" || hold.Agent != "claude" || hold.At != h.Clock.Now() {
		t.Fatalf("hold = %#v", hold)
	}

	h.Driver.PaneScreens["w1:p1"] = readScreenFixture(t, "claude-empty.txt")
	outcomes, err = h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("resumed Drain = %#v, %v", outcomes, err)
	}
	if prompts := h.Driver.PromptLog(); len(prompts) != 1 || !strings.Contains(prompts[0].Text, `redelivery="0"`) {
		t.Fatalf("prompts = %#v", prompts)
	}
	if hold := h.Dispatcher.DraftHold(); hold != nil {
		t.Fatalf("hold survived a clear composer: %#v", hold)
	}
}

func TestDraftGuardReadsNoPaneWithAnEmptyQueue(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}

	if outcomes, err := h.Dispatcher.Drain(context.Background()); err != nil || len(outcomes) != 0 {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if calls := h.Driver.CallLog(); len(calls) != 0 {
		t.Fatalf("guard called herdr with nothing to deliver: %v", calls)
	}
}

func TestDraftGuardDispatchesWhenTheScreenIsUnreadable(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Driver.PaneScreenErr = errors.New("pane read failed")

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if hold := h.Dispatcher.DraftHold(); hold != nil {
		t.Fatalf("guard held on its own read failure: %#v", hold)
	}
}

func TestDraftGuardOffDispatchesIntoADraft(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}
	guard := false
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Store: h.Store, Driver: h.Driver, Target: h.Target, OrgID: h.OrgID,
		PromptTimeout: 5 * time.Second, Now: h.Clock.Now, DraftGuard: &guard,
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}

	outcomes, err := dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if calls := h.Driver.CallLog(); len(calls) != 1 || calls[0] != "prompt:"+h.Target {
		t.Fatalf("calls = %v, want the prompt only", calls)
	}
}

func TestPausedDrainClaimsNothing(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Dispatcher.SetPaused(true)

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 0 {
		t.Fatalf("paused Drain = %#v, %v", outcomes, err)
	}
	if prompts := h.Driver.PromptLog(); len(prompts) != 0 {
		t.Fatalf("paused dispatcher prompted: %#v", prompts)
	}
	// A pause must cost the delivery nothing: same status, no burnt attempt.
	delivery := getTestDelivery(t, h.Store, row.DeliveryID)
	if delivery.Status != DeliveryPending || delivery.AttemptCount != 0 || delivery.LastError != nil {
		t.Fatalf("paused delivery = %#v", delivery)
	}
	if !h.Dispatcher.Paused() {
		t.Fatal("Paused() = false after SetPaused(true)")
	}

	h.Dispatcher.SetPaused(false)
	outcomes, err = h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("resumed Drain = %#v, %v", outcomes, err)
	}
}

func TestDraftHoldNotifiesOncePerHold(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", User: "Dana", Content: "Can you check the batch"})
	h.Enqueue(t, DispatchEnqueueInput{Key: "e2", User: "Sam", Content: "second"})
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}

	if outcomes, err := h.Dispatcher.Drain(context.Background()); err != nil || len(outcomes) != 0 {
		t.Fatalf("held Drain = %#v, %v", outcomes, err)
	}
	notices := h.Driver.NoticeLog()
	if len(notices) != 1 {
		t.Fatalf("notices = %#v, want exactly one", notices)
	}
	if notices[0].Title != "courier: message waiting" {
		t.Fatalf("title = %q", notices[0].Title)
	}
	if notices[0].Body != "Dana on mattermost — 2 waiting until your composer is clear" {
		t.Fatalf("body = %q", notices[0].Body)
	}

	// The hold is observed again on every tick; the human is told once.
	if outcomes, err := h.Dispatcher.Drain(context.Background()); err != nil || len(outcomes) != 0 {
		t.Fatalf("second held Drain = %#v, %v", outcomes, err)
	}
	if notices := h.Driver.NoticeLog(); len(notices) != 1 {
		t.Fatalf("notices = %#v, want no second toast for the same pane", notices)
	}
}

func TestDraftHoldNotifyFailureDoesNotFailTheDrain(t *testing.T) {
	h := dispatchNewHarness(t)
	row := h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}
	h.Driver.NotifyErr = errors.New("no foreground client")

	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 0 {
		t.Fatalf("held Drain = %#v, %v", outcomes, err)
	}
	if hold := h.Dispatcher.DraftHold(); hold == nil || hold.PaneID != "w1:p1" {
		t.Fatalf("hold = %#v, want the hold to survive a failed toast", hold)
	}
	if delivery := getTestDelivery(t, h.Store, row.DeliveryID); delivery.Status != DeliveryPending || delivery.AttemptCount != 0 {
		t.Fatalf("delivery = %#v", delivery)
	}
}

func TestDraftNotifyOffHoldsSilently(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "hello?"})
	h.Driver.PaneScreens = map[string]string{"w1:p1": readScreenFixture(t, "claude-draft.txt")}
	notify := false
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Store: h.Store, Driver: h.Driver, Target: h.Target, OrgID: h.OrgID,
		PromptTimeout: 5 * time.Second, Now: h.Clock.Now, NotifyHolds: &notify,
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}

	if outcomes, err := dispatcher.Drain(context.Background()); err != nil || len(outcomes) != 0 {
		t.Fatalf("held Drain = %#v, %v", outcomes, err)
	}
	if notices := h.Driver.NoticeLog(); len(notices) != 0 {
		t.Fatalf("notices = %#v, want none with notification off", notices)
	}
	if hold := dispatcher.DraftHold(); hold == nil {
		t.Fatal("guard stopped holding when notification was disabled")
	}
}
