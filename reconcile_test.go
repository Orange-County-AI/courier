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

func TestFindRelabelCandidateUsesSessionEvidenceOnly(t *testing.T) {
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

	tests := []struct {
		name         string
		agents       []Agent
		wantSignal   RelabelSignal
		wantMatch    bool
		wantOccupant bool
	}{
		{
			name:       "exact session beats pane",
			agents:     []Agent{{Agent: "omp", PaneID: "wB:p7", Session: &AgentSession{Value: session}}},
			wantSignal: RelabelSession,
			wantMatch:  true,
		},
		{
			name:       "session prefix is value prefix not dirname",
			agents:     []Agent{{Agent: "omp", PaneID: "other", Session: &AgentSession{Value: session + ".continued"}}},
			wantSignal: RelabelSessionPrefix,
			wantMatch:  true,
		},
		{
			name:   "same directory is not identity",
			agents: []Agent{{Agent: "omp", PaneID: "wB:p7", Session: &AgentSession{Value: "/home/dev/.omp/agent/sessions/-projects-acme-widget/B.jsonl"}}},
		},
		{
			name:   "correctly named agent is not a candidate",
			agents: []Agent{{Name: "helper", Agent: "omp", PaneID: pane, Session: &AgentSession{Value: session}}},
		},
		{
			name:         "bare pane is not identity",
			agents:       []Agent{{Agent: "omp", PaneID: pane}},
			wantOccupant: true,
		},
		{
			name:         "sessionless omp at the recorded pane is not adopted",
			agents:       []Agent{{Agent: "omp", PaneID: pane, Session: &AgentSession{}}},
			wantOccupant: true,
		},
		{
			name:         "stranger session at the recorded pane is not adopted",
			agents:       []Agent{{Agent: "omp", PaneID: pane, Session: &AgentSession{Value: "/home/dev/.omp/agent/sessions/-projects-acme-widget/B.jsonl"}}},
			wantOccupant: true,
		},
		{
			name:   "elsewhere without session evidence is not an occupant",
			agents: []Agent{{Agent: "omp", PaneID: "wB:p7"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			found := FindRelabelCandidate(test.agents, state)
			if found.Ambiguous != nil {
				t.Fatalf("FindRelabelCandidate = %#v, want no ambiguity", found)
			}
			if test.wantMatch {
				if found.Match == nil {
					t.Fatalf("FindRelabelCandidate = %#v, want match", found)
				}
				if found.Match.Signal != test.wantSignal {
					t.Errorf("signal = %q, want %q", found.Match.Signal, test.wantSignal)
				}
				if !reflect.DeepEqual(found.Match.Agent, test.agents[0]) {
					t.Errorf("matched agent = %#v, want %#v", found.Match.Agent, test.agents[0])
				}
			} else if found.Match != nil {
				t.Errorf("FindRelabelCandidate = %#v, want no match", found)
			}
			if test.wantOccupant {
				if found.Occupant == nil {
					t.Fatalf("FindRelabelCandidate = %#v, want occupant", found)
				}
				if found.Occupant.PaneID != pane {
					t.Errorf("occupant pane = %q, want %q", found.Occupant.PaneID, pane)
				}
			} else if found.Occupant != nil {
				t.Errorf("occupant = %#v, want nil", found.Occupant)
			}
		})
	}
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

func TestReconcileDoesNotAdoptSessionlessAgentAtRecordedPane(t *testing.T) {
	h := dispatchNewHarness(t)
	h.Driver.Agents = []Agent{{Agent: "claude", Status: "idle", PaneID: "w1:p1", WorkspaceID: "w1"}}
	var logs []string

	result, err := Reconcile(context.Background(), ReconcileOptions{
		Store: h.Store, Driver: h.Driver, OrgID: h.OrgID, Now: h.Clock.Now,
		Log: func(message string) { logs = append(logs, message) },
	})
	if err != nil || result.Action != ReconcileRestartedResume {
		t.Fatalf("Reconcile = %#v, %v", result, err)
	}
	if renames := h.Driver.RenameLog(); len(renames) != 0 {
		t.Fatalf("renames = %#v, want none", renames)
	}
	if starts := h.Driver.StartLog(); len(starts) != 1 {
		t.Fatalf("starts = %#v, want one", starts)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "is an address, not an identity") {
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
