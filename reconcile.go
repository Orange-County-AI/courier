package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type ReconcileAction string

const (
	ReconcileRefreshed       ReconcileAction = "refreshed"
	ReconcileRelabeled       ReconcileAction = "relabeled"
	ReconcileRestartedResume ReconcileAction = "restarted-resume"
	ReconcileRestartedFresh  ReconcileAction = "restarted-fresh"
	ReconcileUnavailable     ReconcileAction = "unavailable"
)

type ReconcileResult struct {
	Action ReconcileAction
	State  *ReconcilerState
	Agent  *Agent
	Error  string
}

type ReconcileOptions struct {
	Store     *Store
	Driver    Driver
	OrgID     string
	ExtraArgs []string
	Log       func(string)
	Now       func() int64
}

type RelabelSignal string

const (
	RelabelSession       RelabelSignal = "session"
	RelabelSessionPrefix RelabelSignal = "session-prefix"
)

type RelabelMatch struct {
	Agent  Agent
	Signal RelabelSignal
}

type RelabelAmbiguity struct {
	Signal RelabelSignal
	Agents []Agent
}

type RelabelSearch struct {
	Match     *RelabelMatch
	Ambiguous *RelabelAmbiguity
	// Occupant is an eligible agent sitting at the pane id this org recorded,
	// reported only when no session evidence identified it. It is never adopted:
	// it exists so the refusal is logged instead of silent.
	Occupant *Agent
}

// FindRelabelCandidate identifies a restored agent whose routing label was
// lost. The tiers are evidence strength, not scoring: the first non-empty tier
// decides, and ambiguity refuses rather than adopting the wrong conversation.
//
// Every tier compares the agent's OWN session reference, and there is no pane
// tier. Measured against herdr 0.8.0 (protocol 19) on 2026-08-17: public pane
// numbering is persisted in ~/.config/herdr/session.json and restored, so wB:p1
// outlives a restart; when that file is lost the counters restart and the same
// wB:p1 is minted for an unrelated agent. herdr's protocol carries no
// server-instance identity to scope the id with (ping answers type, version,
// protocol and capabilities; session.snapshot adds only focus handles), and
// terminal_id is no substitute — clock plus a process-local counter, minted
// fresh on restore, and one terminal can be re-tenanted by another agent
// session. So an agent at the recorded pane is evidence of location, never of
// identity, and courier reports it as an Occupant rather than adopting it.
func FindRelabelCandidate(agents []Agent, state *ReconcilerState) RelabelSearch {
	if state == nil {
		return RelabelSearch{}
	}
	wantSession := reconcilePointerValue(state.NativeSessionValue)
	wantPane := reconcilePointerValue(state.PaneID)

	eligible := make([]Agent, 0, len(agents))
	for _, agent := range agents {
		if agent.Agent != "" && agent.Agent != state.AgentKind {
			continue
		}
		if agent.Name == state.PaneLabel {
			continue
		}
		eligible = append(eligible, agent)
	}

	tiers := []struct {
		signal RelabelSignal
		match  func(Agent) bool
	}{
		{
			signal: RelabelSession,
			match: func(agent Agent) bool {
				return wantSession != "" && reconcileSessionValue(agent) == wantSession
			},
		},
		{
			signal: RelabelSessionPrefix,
			match: func(agent Agent) bool {
				value := reconcileSessionValue(agent)
				if wantSession == "" || value == "" || value == wantSession {
					return false
				}
				return strings.HasPrefix(value, wantSession) || strings.HasPrefix(wantSession, value)
			},
		},
	}

	for _, tier := range tiers {
		hits := make([]Agent, 0, len(eligible))
		for _, agent := range eligible {
			if tier.match(agent) {
				hits = append(hits, agent)
			}
		}
		switch len(hits) {
		case 1:
			return RelabelSearch{Match: &RelabelMatch{Agent: hits[0], Signal: tier.signal}}
		case 0:
			continue
		default:
			return RelabelSearch{Ambiguous: &RelabelAmbiguity{Signal: tier.signal, Agents: hits}}
		}
	}
	if wantPane != "" {
		for _, agent := range eligible {
			if agent.PaneID == wantPane {
				occupant := agent
				return RelabelSearch{Occupant: &occupant}
			}
		}
	}
	return RelabelSearch{}
}

func Reconcile(ctx context.Context, opts ReconcileOptions) (ReconcileResult, error) {
	log := opts.Log
	if log == nil {
		log = func(string) {}
	}
	now := opts.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	if opts.Store == nil || opts.Driver == nil {
		return ReconcileResult{}, fmt.Errorf("reconcile: store and driver are required")
	}

	state, err := opts.Store.GetReconcilerState(opts.OrgID)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("get reconciler state: %w", err)
	}
	if state == nil {
		return ReconcileResult{
			Action: ReconcileUnavailable,
			Error:  fmt.Sprintf("no reconciler_state row for org %s — the host is not provisioned for it", opts.OrgID),
		}, nil
	}

	existing, err := opts.Driver.GetAgent(ctx, state.PaneLabel)
	if err != nil {
		// An unreachable herdr is not evidence that the agent is absent. Starting
		// on a socket blip can leave two agents in one pane.
		log(fmt.Sprintf("reconcile: herdr unreachable for %s — %v", state.PaneLabel, err))
		return reconcileUnavailable(state, err.Error()), nil
	}

	mismatched := existing != nil && existing.Agent != "" && existing.Agent != state.AgentKind
	if existing != nil && !mismatched {
		next, err := reconcilePutStateFromAgent(opts.Store, state, existing, state.SessionGeneration, now(), true)
		if err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{Action: ReconcileRefreshed, State: next, Agent: existing}, nil
	}

	if existing == nil {
		result, decided, err := reconcileReacquire(ctx, opts, state, log, now)
		if err != nil {
			return ReconcileResult{}, err
		}
		if decided {
			return result, nil
		}
	}

	if mismatched {
		log(fmt.Sprintf("reconcile: %s is running %s, expected %s — restarting it", state.PaneLabel, existing.Agent, state.AgentKind))
	}
	// The one surviving use of a stored pane id, and it is an address to launch
	// into rather than proof of identity: what starts here is courier's own agent
	// under its own label. If the id has been re-minted for someone else, herdr
	// answers agent_pane_busy or agent_pane_not_found and this reports unavailable.
	if state.PaneID == nil || *state.PaneID == "" {
		return reconcileUnavailable(state, fmt.Sprintf("agent %s is absent and reconciler_state has no pane_id to start it in", state.PaneLabel)), nil
	}

	extra := append([]string(nil), opts.ExtraArgs...)
	var started *Agent
	action := ReconcileRestartedFresh
	if resume := reconcilePointerValue(state.NativeSessionValue); resume != "" {
		resumeArgs := append(append([]string(nil), extra...), "--resume="+resume)
		started, err = opts.Driver.StartAgent(ctx, state.PaneLabel, state.AgentKind, *state.PaneID, resumeArgs)
		if err == nil {
			action = ReconcileRestartedResume
		} else {
			// Resume ids can be permanently invalid after transcript pruning or a
			// harness format change. Fall back, but make the lost continuity loud.
			log(fmt.Sprintf("reconcile: resume of %s failed — %v — starting fresh", resume, err))
		}
	}
	if started == nil {
		started, err = opts.Driver.StartAgent(ctx, state.PaneLabel, state.AgentKind, *state.PaneID, extra)
		if err != nil {
			log(fmt.Sprintf("reconcile: could not start %s — %v", state.PaneLabel, err))
			return reconcileUnavailable(state, err.Error()), nil
		}
		action = ReconcileRestartedFresh
	}

	generation, err := opts.Store.BumpGeneration(opts.OrgID, now())
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("bump reconciler generation: %w", err)
	}
	next, err := reconcilePutStateFromAgent(opts.Store, state, started, generation, now(), false)
	if err != nil {
		return ReconcileResult{}, err
	}
	log(fmt.Sprintf("reconcile: %s %s — generation %d", state.PaneLabel, action, generation))
	return ReconcileResult{Action: action, State: next, Agent: started}, nil
}

func reconcileReacquire(
	ctx context.Context,
	opts ReconcileOptions,
	state *ReconcilerState,
	log func(string),
	now func() int64,
) (ReconcileResult, bool, error) {
	agents, err := opts.Driver.ListAgents(ctx)
	if err != nil {
		// Listing is an optional recovery probe. The start path below is bounded,
		// not safe: herdr answers agent_pane_busy only when the target pane
		// already hosts an agent or is not its own idle foreground shell, and
		// agent_pane_not_found when the id no longer resolves. An idle shell in
		// any workspace is accepted, so the worst case is courier's own agent
		// started in the wrong pane — never a stranger answering this org's chat.
		log(fmt.Sprintf("reconcile: could not list agents to re-acquire %s — %v", state.PaneLabel, err))
		return ReconcileResult{}, false, nil
	}
	search := FindRelabelCandidate(agents, state)
	if search.Ambiguous != nil {
		which := make([]string, 0, len(search.Ambiguous.Agents))
		for _, agent := range search.Ambiguous.Agents {
			handle := agent.Name
			if handle == "" {
				handle = agent.PaneID
			}
			if handle == "" {
				handle = "<unnamed>"
			}
			which = append(which, handle)
		}
		message := fmt.Sprintf("%d agents match %s on %s (%s) — refusing to guess which one this org addresses",
			len(search.Ambiguous.Agents), state.PaneLabel, search.Ambiguous.Signal, strings.Join(which, ", "))
		log("reconcile: AMBIGUOUS — " + message)
		return reconcileUnavailable(state, message), true, nil
	}
	if search.Match == nil {
		if search.Occupant != nil {
			handle := search.Occupant.Name
			if handle == "" {
				handle = "<unnamed>"
			}
			log(fmt.Sprintf("reconcile: %s occupies %s but nothing identifies it as the agent for %s — a pane id is an address, not an identity, so it is NOT adopted",
				handle, reconcilePointerValue(state.PaneID), state.PaneLabel))
		}
		return ReconcileResult{}, false, nil
	}

	handle := search.Match.Agent.Name
	if handle == "" {
		handle = search.Match.Agent.PaneID
	}
	if handle == "" {
		log(fmt.Sprintf("reconcile: found the agent for %s but it has neither a name nor a pane_id to rename by", state.PaneLabel))
		return ReconcileResult{}, false, nil
	}
	if _, err := opts.Driver.RenameAgent(ctx, handle, state.PaneLabel); err != nil {
		log(fmt.Sprintf("reconcile: rename of %s to %s failed — %v", handle, state.PaneLabel, err))
		return reconcileUnavailable(state, err.Error()), true, nil
	}

	// Verify rather than assume. The result that matters is whether the stable
	// target resolves again, not whether rename returned without an error.
	renamed, err := opts.Driver.GetAgent(ctx, state.PaneLabel)
	if err != nil {
		return reconcileUnavailable(state, "rename verification failed: "+err.Error()), true, nil
	}
	if renamed == nil {
		message := fmt.Sprintf("renamed %s to %s but herdr still reports agent_not_found", handle, state.PaneLabel)
		log("reconcile: " + message)
		return reconcileUnavailable(state, message), true, nil
	}

	generation, err := opts.Store.BumpGeneration(opts.OrgID, now())
	if err != nil {
		return ReconcileResult{}, false, fmt.Errorf("bump reconciler generation: %w", err)
	}
	next, err := reconcilePutStateFromAgent(opts.Store, state, renamed, generation, now(), true)
	if err != nil {
		return ReconcileResult{}, false, err
	}
	log(fmt.Sprintf("reconcile: re-acquired label %s — renamed %s (matched on %s) — generation %d",
		state.PaneLabel, handle, search.Match.Signal, generation))
	return ReconcileResult{Action: ReconcileRelabeled, State: next, Agent: renamed}, true, nil
}

func reconcilePutStateFromAgent(store *Store, state *ReconcilerState, agent *Agent, generation int64, now int64, refreshKind bool) (*ReconcilerState, error) {
	input := ReconcilerStateInput{
		OrgID:               state.OrgID,
		WorkspaceID:         state.WorkspaceID,
		PaneID:              state.PaneID,
		PaneLabel:           state.PaneLabel,
		AgentKind:           state.AgentKind,
		NativeSessionSource: state.NativeSessionSource,
		NativeSessionKind:   state.NativeSessionKind,
		NativeSessionValue:  state.NativeSessionValue,
		SessionGeneration:   &generation,
	}
	if agent != nil {
		if agent.WorkspaceID != "" {
			input.WorkspaceID = reconcileStringPointer(agent.WorkspaceID)
		}
		if agent.PaneID != "" {
			input.PaneID = reconcileStringPointer(agent.PaneID)
		}
		if refreshKind && agent.Agent != "" {
			input.AgentKind = agent.Agent
		}
		if agent.Session != nil {
			if agent.Session.Source != "" {
				input.NativeSessionSource = reconcileStringPointer(agent.Session.Source)
			}
			if agent.Session.Kind != "" {
				input.NativeSessionKind = reconcileStringPointer(agent.Session.Kind)
			}
			if agent.Session.Value != "" {
				input.NativeSessionValue = reconcileStringPointer(agent.Session.Value)
			}
		}
	}
	next, err := store.PutReconcilerState(input, now)
	if err != nil {
		return nil, fmt.Errorf("put reconciler state: %w", err)
	}
	return next, nil
}

func reconcileUnavailable(state *ReconcilerState, message string) ReconcileResult {
	return ReconcileResult{Action: ReconcileUnavailable, State: state, Error: message}
}

func reconcilePointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func reconcileStringPointer(value string) *string {
	return &value
}

func reconcileSessionValue(agent Agent) string {
	if agent.Session == nil {
		return ""
	}
	return agent.Session.Value
}
