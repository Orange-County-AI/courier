package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReconcileRelabelsRestoredAgentBySession(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{dispatchRestoredAgent(Agent{})}

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRelabeled {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if renames := h.Driver.RenameLog(); !reflect.DeepEqual(renames, []DispatchFakeRename{{Target: "w1:p1", Name: h.Target}}) {
		t.Fatalf("renames = %#v", renames)
	}
	if starts := h.Driver.StartLog(); len(starts) != 0 {
		t.Fatalf("started duplicate agent: %#v", starts)
	}
	state, err := h.Store.GetReconcilerState(h.OrgID)
	if err != nil || state == nil || state.PaneLabel != h.Target || reconcilePointerValue(state.NativeSessionValue) != "sess-1" || state.SessionGeneration != 1 {
		t.Fatalf("state = %#v, %v", state, err)
	}
	agent, err := h.Driver.GetAgent(context.Background(), h.Target)
	if err != nil || agent == nil {
		t.Fatalf("renamed target does not resolve: %#v, %v", agent, err)
	}
}

func TestReconcileReportsRenameThatDoesNotStick(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{dispatchRestoredAgent(Agent{Name: "someone-else"})}
	h.Driver.RenameDoesNotStick = true

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileUnavailable || !strings.Contains(result.Error, "agent_not_found") {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if starts := h.Driver.StartLog(); len(starts) != 0 {
		t.Fatalf("started after unverified rename: %#v", starts)
	}
}

func TestDispatcherStartRelabelsBeforeDelivery(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Enqueue(t, DispatchEnqueueInput{Key: "e1", Content: "are you there?"})
	h.Driver.Agents = []Agent{dispatchRestoredAgent(Agent{})}

	if result, err := h.Dispatcher.Start(context.Background()); err != nil || result.Action != ReconcileRelabeled {
		t.Fatalf("Start = %#v, %v", result, err)
	}
	outcomes, err := h.Dispatcher.Drain(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].OK {
		t.Fatalf("Drain = %#v, %v", outcomes, err)
	}
	if prompts := h.Driver.PromptLog(); len(prompts) != 1 || prompts[0].Target != h.Target {
		t.Fatalf("prompts = %#v", prompts)
	}
}

func TestReconcileAmbiguousRelabelRefusesTerminally(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{
		dispatchRestoredAgent(Agent{PaneID: "w1:p1"}),
		dispatchRestoredAgent(Agent{PaneID: "w9:p9"}),
	}

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileUnavailable || !strings.Contains(result.Error, "refusing to guess") {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if len(h.Driver.RenameLog()) != 0 || len(h.Driver.StartLog()) != 0 {
		t.Fatalf("ambiguous recovery mutated fleet: renames=%#v starts=%#v", h.Driver.RenameLog(), h.Driver.StartLog())
	}
}

func TestReconcileRejectsPaneWithContradictingSession(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{{
		Agent: "claude", Status: "idle", PaneID: "w1:p1", WorkspaceID: "w1",
		Session: &AgentSession{Kind: "id", Value: "sess-somebody-else"},
	}}

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRestartedResume {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if len(h.Driver.RenameLog()) != 0 || len(h.Driver.StartLog()) != 1 {
		t.Fatalf("recovery = renames %#v starts %#v", h.Driver.RenameLog(), h.Driver.StartLog())
	}
}

func TestReconcileRejectsDifferentHarnessKind(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{dispatchRestoredAgent(Agent{Agent: "codex"})}

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRestartedResume {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if len(h.Driver.RenameLog()) != 0 || len(h.Driver.StartLog()) != 1 {
		t.Fatalf("recovery = renames %#v starts %#v", h.Driver.RenameLog(), h.Driver.StartLog())
	}
	state, err := h.Store.GetReconcilerState(h.OrgID)
	if err != nil || state.AgentKind != "claude" {
		t.Fatalf("restart changed configured kind: %#v, %v", state, err)
	}
}

func TestReconcileStartsMissingAgentWithResume(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = nil

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRestartedResume {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	starts := h.Driver.StartLog()
	if len(starts) != 1 || !reflect.DeepEqual(starts[0].ExtraArgs, []string{"--resume=sess-1"}) || len(h.Driver.RenameLog()) != 0 {
		t.Fatalf("starts/renames = %#v/%#v", starts, h.Driver.RenameLog())
	}
}

func TestReconcileListFailureFallsThroughToSafeStart(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = nil
	h.Driver.ListAgentsErr = errors.New("connect ENOENT /dev/shm/herdr/herdr.sock")

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRestartedResume {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if len(h.Driver.RenameLog()) != 0 || len(h.Driver.StartLog()) != 1 {
		t.Fatalf("recovery = renames %#v starts %#v", h.Driver.RenameLog(), h.Driver.StartLog())
	}
}

func TestFindRelabelCandidateUsesIdentityTiers(t *testing.T) {
	workspace := "w1"
	pane := "w1:p1"
	source := "herdr:omp"
	kind := "path"
	session := "/home/dev/.omp/agent/sessions/-projects-acme-widget/A.jsonl"
	state := &ReconcilerState{
		OrgID: "o", WorkspaceID: &workspace, PaneID: &pane, PaneLabel: "helper", AgentKind: "omp",
		NativeSessionSource: &source, NativeSessionKind: &kind, NativeSessionValue: &session,
		SessionGeneration: 3,
	}

	t.Run("exact session beats pane", func(t *testing.T) {
		found := FindRelabelCandidate([]Agent{{
			Agent: "omp", PaneID: "wB:p7", Session: &AgentSession{Value: session},
		}}, state)
		if found.Match == nil || found.Match.Signal != RelabelSession {
			t.Fatalf("found = %#v", found)
		}
	})

	t.Run("same directory is not identity", func(t *testing.T) {
		sibling := "/home/dev/.omp/agent/sessions/-projects-acme-widget/B.jsonl"
		found := FindRelabelCandidate([]Agent{{
			Agent: "omp", PaneID: "wB:p7", Session: &AgentSession{Value: sibling},
		}}, state)
		if found.Match != nil || found.Ambiguous != nil {
			t.Fatalf("found = %#v", found)
		}
	})

	t.Run("bare pane is weakest accepted signal", func(t *testing.T) {
		found := FindRelabelCandidate([]Agent{{Agent: "omp", PaneID: pane}}, state)
		if found.Match == nil || found.Match.Signal != RelabelPane {
			t.Fatalf("found = %#v", found)
		}
	})

	t.Run("correctly named agent is not a candidate", func(t *testing.T) {
		found := FindRelabelCandidate([]Agent{{
			Name: "helper", Agent: "omp", PaneID: pane, Session: &AgentSession{Value: session},
		}}, state)
		if found.Match != nil {
			t.Fatalf("found = %#v", found)
		}
	})

	t.Run("session prefix is value prefix not dirname", func(t *testing.T) {
		found := FindRelabelCandidate([]Agent{{
			Agent: "omp", PaneID: "other", Session: &AgentSession{Value: session + ".continued"},
		}}, state)
		if found.Match == nil || found.Match.Signal != RelabelSessionPrefix {
			t.Fatalf("found = %#v", found)
		}
	})
}

func TestReconcileRefreshesNativeSessionOnEveryLook(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{{
		Name: h.Target, Agent: "claude", PaneID: "w1:p1", WorkspaceID: "w1",
		Session: &AgentSession{Agent: "claude", Kind: "id", Source: "herdr:claude", Value: "sess-new"},
	}}

	result, err := h.Dispatcher.Reconcile(context.Background())
	if err != nil || result.Action != ReconcileRefreshed || result.State == nil || reconcilePointerValue(result.State.NativeSessionValue) != "sess-new" {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
}

func TestReconcileResumeFailureFallsBackFreshAndLogs(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = nil
	h.Driver.StartErrors = []error{errors.New("resume transcript missing")}
	var logs []string

	result, err := Reconcile(context.Background(), ReconcileOptions{
		Store: h.Store, Driver: h.Driver, OrgID: h.OrgID, Now: h.Clock.Now,
		Log: func(message string) { logs = append(logs, message) },
	})
	if err != nil || result.Action != ReconcileRestartedFresh {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	starts := h.Driver.StartLog()
	if len(starts) != 2 || !reflect.DeepEqual(starts[0].ExtraArgs, []string{"--resume=sess-1"}) || len(starts[1].ExtraArgs) != 0 {
		t.Fatalf("starts = %#v", starts)
	}
	if len(logs) == 0 || !strings.Contains(strings.Join(logs, "\n"), "starting fresh") {
		t.Fatalf("logs = %v", logs)
	}
}

func TestReconcileWithoutProvisionedStateRefuses(t *testing.T) {
	store := openTestStore(t)
	driver := &FakeDriver{}
	result, err := Reconcile(context.Background(), ReconcileOptions{
		Store: store, Driver: driver, OrgID: "missing", Now: func() int64 { return 1 },
	})
	if err != nil || result.Action != ReconcileUnavailable || result.State != nil || !strings.Contains(result.Error, "not provisioned") {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if calls := driver.CallLog(); len(calls) != 0 {
		t.Fatalf("unprovisioned reconcile called herdr: %v", calls)
	}
}
