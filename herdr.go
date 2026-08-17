package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Protocol 19 was measured against herdr 0.8.0. herdr's docs report 20 for
	// an unreleased build; it is not accepted until an operator opts in or
	// courier ships that compatibility.
	HerdrProtocolVersion = 19

	DefaultPromptTimeout = 120 * time.Second

	socketIOTimeout   = 30 * time.Second
	callTimeoutGrace  = 30 * time.Second
	agentStartTimeout = 30 * time.Second
)

// Driver is the whole transport surface between courier and herdr.
type Driver interface {
	PromptAgent(ctx context.Context, target, text string, timeout time.Duration) PromptResult
	GetAgent(ctx context.Context, target string) (*Agent, error)
	ListAgents(ctx context.Context) ([]Agent, error)
	RenameAgent(ctx context.Context, target, name string) (*Agent, error)
	StartAgent(ctx context.Context, name, kind, paneID string, extraArgs []string) (*Agent, error)
	PaneWaitOutput(ctx context.Context, paneID, match string, timeout time.Duration, regex bool) bool
	SendKeys(ctx context.Context, paneID string, keys []string) bool
	PaneRead(ctx context.Context, paneID string, lines int) (string, error)
	PaneScreen(ctx context.Context, paneID string) (string, error)
}

type AgentSession struct {
	Source string `json:"source"`
	Agent  string `json:"agent"`
	Kind   string `json:"kind"`
	Value  string `json:"value"`
}

// Agent mirrors the herdr 0.8.0 AgentInfo fields courier needs. Nullable JSON
// strings intentionally become empty strings; all identifiers courier acts on
// are required to be non-empty at their use sites.
type Agent struct {
	Name                   string            `json:"name"`
	Agent                  string            `json:"agent"`
	Status                 string            `json:"agent_status"`
	Session                *AgentSession     `json:"agent_session"`
	PaneID                 string            `json:"pane_id"`
	TabID                  string            `json:"tab_id"`
	WorkspaceID            string            `json:"workspace_id"`
	TerminalID             string            `json:"terminal_id"`
	CWD                    string            `json:"cwd"`
	ForegroundCWD          string            `json:"foreground_cwd"`
	DisplayAgent           string            `json:"display_agent"`
	Title                  string            `json:"title"`
	TerminalTitle          string            `json:"terminal_title"`
	TerminalTitleStripped  string            `json:"terminal_title_stripped"`
	Focused                bool              `json:"focused"`
	InteractiveReady       bool              `json:"interactive_ready"`
	LaunchPending          bool              `json:"launch_pending"`
	ScreenDetectionSkipped bool              `json:"screen_detection_skipped"`
	Revision               uint64            `json:"revision"`
	StateChangeSeq         *uint64           `json:"state_change_seq"`
	StateLabels            map[string]string `json:"state_labels"`
	Tokens                 map[string]string `json:"tokens"`
}

type PromptResult struct {
	OK      bool
	Blocked bool
	Code    string
	Error   string
}

type SocketDriverOptions struct {
	// SocketPath is the explicit low-level override. Session is the equivalent
	// of an explicit --session and is used only when SocketPath is empty.
	SocketPath string
	Session    string
	Log        func(string)
}

// SocketDriver serializes calls so request ids and protocol logging remain
// deterministic. Herdr serves exactly one request per accepted connection, so
// each protocol check and operation gets its own socket. Mutating operations
// are never silently replayed after a transport failure.
type SocketDriver struct {
	path              string
	acceptedProtocols map[int]struct{}
	log               func(string)

	mu             sync.Mutex
	nextID         uint64
	loggedProtocol int

	stallPollInterval time.Duration
	stallPollAttempts int
	startPollInterval time.Duration
}

var _ Driver = (*SocketDriver)(nil)

// NewSocketDriver resolves configuration without dialing. The first operation
// connects and verifies the server protocol; every reconnect does the same.
func NewSocketDriver(_ context.Context, opts SocketDriverOptions) (*SocketDriver, error) {
	path, err := resolveHerdrSocketPath(opts)
	if err != nil {
		return nil, err
	}
	accepted, err := acceptedHerdrProtocols(os.Getenv("COURIER_HERDR_PROTOCOL_ALLOW"))
	if err != nil {
		return nil, err
	}
	logAccepted := opts.Log
	if logAccepted == nil {
		logAccepted = func(message string) { log.Print(message) }
	}
	return &SocketDriver{
		path:              path,
		acceptedProtocols: accepted,
		log:               logAccepted,
		stallPollInterval: time.Second,
		stallPollAttempts: 15,
		startPollInterval: time.Second,
	}, nil
}

func (d *SocketDriver) SocketPath() string { return d.path }

func (d *SocketDriver) Close() error { return nil }

func resolveHerdrSocketPath(opts SocketDriverOptions) (string, error) {
	if opts.SocketPath != "" {
		return opts.SocketPath, nil
	}
	if opts.Session != "" {
		return socketPathForSession(opts.Session)
	}
	if path := os.Getenv("HERDR_SOCKET_PATH"); path != "" {
		return path, nil
	}
	if session := os.Getenv("HERDR_SESSION"); session != "" {
		return socketPathForSession(session)
	}
	return socketPathForSession("")
}

func acceptedHerdrProtocols(extra string) (map[int]struct{}, error) {
	accepted := map[int]struct{}{HerdrProtocolVersion: {}}
	for _, token := range strings.Split(extra, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		version, err := strconv.ParseUint(token, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("COURIER_HERDR_PROTOCOL_ALLOW contains invalid protocol %q", token)
		}
		accepted[int(version)] = struct{}{}
	}
	return accepted, nil
}

func sortedProtocolVersions(accepted map[int]struct{}) []int {
	versions := make([]int, 0, len(accepted))
	for version := range accepted {
		versions = append(versions, version)
	}
	sort.Ints(versions)
	return versions
}

func socketPathForSession(session string) (string, error) {
	if session == "default" {
		session = ""
	}
	if session != "" {
		if len(session) > 64 {
			return "", errors.New("herdr session name cannot be longer than 64 bytes")
		}
		if session == "." || session == ".." {
			return "", errors.New("herdr session name cannot be . or ..")
		}
		for _, b := range session {
			if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '.' || b == '_' || b == '-') {
				return "", errors.New("herdr session name may only contain ASCII letters, numbers, '.', '_' and '-'")
			}
		}
	}

	configHome := os.Getenv("XDG_CONFIG_HOME")
	if configHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve herdr config directory: %w", err)
		}
		configHome = filepath.Join(home, ".config")
	}
	base := filepath.Join(configHome, "herdr")
	if session != "" {
		base = filepath.Join(base, "sessions", session)
	}
	return filepath.Join(base, "herdr.sock"), nil
}

type wireRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

type wireResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *herdrAPIError  `json:"error"`
}

type herdrAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *herdrAPIError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	return "unknown herdr API error"
}

type pingResult struct {
	Type     string `json:"type"`
	Version  string `json:"version"`
	Protocol int    `json:"protocol"`
}

func (d *SocketDriver) dial(ctx context.Context) (net.Conn, *json.Encoder, *json.Decoder, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", d.path)
	if err != nil {
		return nil, nil, nil, err
	}
	return conn, json.NewEncoder(conn), json.NewDecoder(conn), nil
}

func (d *SocketDriver) verifyProtocolLocked(ctx context.Context) error {
	conn, encoder, decoder, err := d.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	response, err := d.exchange(ctx, conn, encoder, decoder, "ping", struct{}{})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return response.Error
	}
	var pong pingResult
	if len(response.Result) == 0 || json.Unmarshal(response.Result, &pong) != nil || pong.Type != "pong" {
		return errors.New("ping returned an unrecognized response")
	}
	if _, accepted := d.acceptedProtocols[pong.Protocol]; !accepted {
		return fmt.Errorf("protocol mismatch: server %d, courier accepts %v", pong.Protocol, sortedProtocolVersions(d.acceptedProtocols))
	}
	if d.loggedProtocol != pong.Protocol {
		d.log(fmt.Sprintf("herdr protocol %d accepted", pong.Protocol))
		d.loggedProtocol = pong.Protocol
	}
	return nil
}

func (d *SocketDriver) exchange(ctx context.Context, conn net.Conn, encoder *json.Encoder, decoder *json.Decoder, method string, params any) (wireResponse, error) {
	d.nextID++
	id := fmt.Sprintf("req_%d", d.nextID)
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return wireResponse{}, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer func() {
		if stop() {
			_ = conn.SetDeadline(time.Time{})
		}
	}()

	if err := encoder.Encode(wireRequest{ID: id, Method: method, Params: params}); err != nil {
		return wireResponse{}, err
	}
	var response wireResponse
	if err := decoder.Decode(&response); err != nil {
		return wireResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return wireResponse{}, err
	}
	if response.ID != id {
		return wireResponse{}, fmt.Errorf("response id %q does not match request id %q", response.ID, id)
	}
	if response.Error == nil && len(response.Result) == 0 {
		return wireResponse{}, errors.New("response contains neither result nor error")
	}
	return response, nil
}

func (d *SocketDriver) call(ctx context.Context, method string, params any, bound time.Duration) (json.RawMessage, error) {
	if bound <= 0 {
		bound = socketIOTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyProtocolLocked(callCtx); err != nil {
		return nil, err
	}
	conn, encoder, decoder, err := d.dial(callCtx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := d.exchange(callCtx, conn, encoder, decoder, method, params)
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}
	return response.Result, nil
}

func timeoutWithGrace(timeout time.Duration) time.Duration {
	if timeout > time.Duration(math.MaxInt64)-callTimeoutGrace {
		return timeout
	}
	return timeout + callTimeoutGrace
}

func durationMillis(timeout time.Duration) int64 {
	ms := timeout.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

type agentResponse struct {
	Type  string `json:"type"`
	Agent *Agent `json:"agent"`
}

func decodeAgentResponse(raw json.RawMessage, allowedTypes ...string) (*Agent, error) {
	var response agentResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	allowed := false
	for _, resultType := range allowedTypes {
		if response.Type == resultType {
			allowed = true
			break
		}
	}
	if !allowed || response.Agent == nil {
		return nil, fmt.Errorf("unexpected %q response without an agent", response.Type)
	}
	return response.Agent, nil
}

func promptFailure(err error) PromptResult {
	var apiErr *herdrAPIError
	if errors.As(err, &apiErr) {
		return PromptResult{Code: apiErr.Code, Error: apiErr.Error()}
	}
	return PromptResult{Error: err.Error()}
}

// PromptAgent submits one prompt and waits for idle, done, or blocked. Blocked
// means the prompt was consumed but no answer was produced, so it remains an
// explicit non-success signal to the dispatcher even though the API call won.
func (d *SocketDriver) PromptAgent(ctx context.Context, target, text string, timeout time.Duration) PromptResult {
	if timeout <= 0 {
		timeout = DefaultPromptTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeoutWithGrace(timeout))
	defer cancel()

	params := map[string]any{
		"target": target,
		"text":   text,
		"wait": map[string]any{
			"until":      []string{"idle", "done", "blocked"},
			"timeout_ms": durationMillis(timeout),
		},
	}
	raw, err := d.call(callCtx, "agent.prompt", params, timeoutWithGrace(timeout))
	if err != nil {
		var apiErr *herdrAPIError
		if errors.As(err, &apiErr) && apiErr.Code == "agent_prompt_stalled" {
			if recovered, definitive := d.flushPastedPrompt(callCtx, target, timeout); definitive {
				return recovered
			}
		}
		return promptFailure(err)
	}
	agent, err := decodeAgentResponse(raw, "agent_prompted")
	if err != nil {
		return PromptResult{Error: fmt.Sprintf("herdr agent prompt %s: %v", target, err)}
	}
	return PromptResult{OK: true, Blocked: agent.Status == "blocked"}
}

// flushPastedPrompt ports a measured recovery: large bracketed pastes can
// collapse into an OMP attachment chip and absorb
// herdr's submit key. One Enter is safe on an empty input, but accepting the key
// proves nothing, so state_change_seq must move before courier waits or reports
// success. A failed precondition returns definitive=false so the original stall
// remains the reported cause.
func (d *SocketDriver) flushPastedPrompt(ctx context.Context, target string, timeout time.Duration) (PromptResult, bool) {
	agent, err := d.GetAgent(ctx, target)
	if err != nil || agent == nil || agent.PaneID == "" {
		return PromptResult{}, false
	}
	before := agent.StateChangeSeq
	if !d.SendKeys(ctx, agent.PaneID, []string{"Enter"}) {
		return PromptResult{}, false
	}

	moved := false
	for range d.stallPollAttempts {
		timer := time.NewTimer(d.stallPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return PromptResult{}, false
		case <-timer.C:
		}
		now, err := d.GetAgent(ctx, target)
		if err != nil || now == nil {
			return PromptResult{}, false
		}
		if before == nil || now.StateChangeSeq == nil || *now.StateChangeSeq != *before {
			moved = true
			break
		}
	}
	if !moved {
		return PromptResult{}, false
	}

	params := map[string]any{
		"target":     target,
		"until":      []string{"idle", "done", "blocked"},
		"timeout_ms": durationMillis(timeout),
	}
	raw, err := d.call(ctx, "agent.wait", params, timeoutWithGrace(timeout))
	if err != nil {
		return promptFailure(err), true
	}
	settled, err := decodeAgentResponse(raw, "agent_info")
	if err != nil {
		return PromptResult{Error: fmt.Sprintf("herdr agent wait %s: %v", target, err)}, true
	}
	return PromptResult{OK: true, Blocked: settled.Status == "blocked"}, true
}

func (d *SocketDriver) GetAgent(ctx context.Context, target string) (*Agent, error) {
	raw, err := d.call(ctx, "agent.get", map[string]any{"target": target}, socketIOTimeout)
	if err != nil {
		var apiErr *herdrAPIError
		if errors.As(err, &apiErr) && apiErr.Code == "agent_not_found" {
			return nil, nil
		}
		return nil, fmt.Errorf("herdr agent get %s: %w", target, err)
	}
	agent, err := decodeAgentResponse(raw, "agent_info")
	if err != nil {
		return nil, fmt.Errorf("herdr agent get %s: %w", target, err)
	}
	return agent, nil
}

func (d *SocketDriver) ListAgents(ctx context.Context) ([]Agent, error) {
	raw, err := d.call(ctx, "agent.list", struct{}{}, socketIOTimeout)
	if err != nil {
		return nil, fmt.Errorf("herdr agent list: %w", err)
	}
	var response struct {
		Type   string          `json:"type"`
		Agents json.RawMessage `json:"agents"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("herdr agent list: %w", err)
	}
	if response.Type != "agent_list" || len(response.Agents) == 0 || string(response.Agents) == "null" {
		return nil, errors.New("herdr agent list: success response has no agents array")
	}
	var agents []Agent
	if err := json.Unmarshal(response.Agents, &agents); err != nil {
		return nil, fmt.Errorf("herdr agent list: %w", err)
	}
	if agents == nil {
		agents = []Agent{}
	}
	return agents, nil
}

func (d *SocketDriver) RenameAgent(ctx context.Context, target, name string) (*Agent, error) {
	raw, err := d.call(ctx, "agent.rename", map[string]any{"target": target, "name": name}, socketIOTimeout)
	if err != nil {
		return nil, fmt.Errorf("herdr agent rename %s %s: %w", target, name, err)
	}
	agent, err := decodeAgentResponse(raw, "agent_info")
	if err != nil {
		return nil, fmt.Errorf("herdr agent rename %s %s: %w", target, name, err)
	}
	return agent, nil
}

// StartAgent preserves the CLI driver's success contract rather than exposing
// raw agent.start's earlier acknowledgement: the named process must still own
// the same terminal and become interactive before this method returns.
func (d *SocketDriver) StartAgent(ctx context.Context, name, kind, paneID string, extraArgs []string) (*Agent, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeoutWithGrace(agentStartTimeout))
	defer cancel()
	params := map[string]any{
		"name":       name,
		"kind":       kind,
		"pane_id":    paneID,
		"timeout_ms": durationMillis(agentStartTimeout),
	}
	if len(extraArgs) > 0 {
		params["args"] = extraArgs
	}
	raw, err := d.call(callCtx, "agent.start", params, timeoutWithGrace(agentStartTimeout))
	if err != nil {
		return nil, fmt.Errorf("herdr agent start %s: %w", name, err)
	}
	started, err := decodeAgentResponse(raw, "agent_started")
	if err != nil {
		return nil, fmt.Errorf("herdr agent start %s: %w", name, err)
	}
	if started.TerminalID == "" {
		return nil, fmt.Errorf("herdr agent start %s: response did not include terminal_id", name)
	}

	deadline := time.Now().Add(agentStartTimeout)
	for {
		if time.Now().After(deadline) {
			_, _ = d.GetAgent(callCtx, name)
			return nil, fmt.Errorf("herdr agent start %s: timed out waiting for agent startup", name)
		}
		agent, err := d.GetAgent(callCtx, name)
		if err != nil {
			return nil, fmt.Errorf("herdr agent start %s: %w", name, err)
		}
		if agent == nil {
			agent, err = d.GetAgent(callCtx, paneID)
			if err != nil {
				return nil, fmt.Errorf("herdr agent start %s: %w", name, err)
			}
		}
		if agent != nil {
			switch {
			case agent.TerminalID != started.TerminalID:
				return nil, fmt.Errorf("herdr agent start %s: named agent no longer owns the target terminal", name)
			case agent.Agent != "" && agent.Agent != kind:
				return nil, fmt.Errorf("herdr agent start %s: expected %s, detected %s", name, kind, agent.Agent)
			case agent.Name != name:
				return nil, fmt.Errorf("herdr agent start %s: named agent no longer owns the target terminal", name)
			case agent.InteractiveReady:
				return agent, nil
			case !agent.LaunchPending:
				return nil, fmt.Errorf("herdr agent start %s: agent process exited before becoming interactive", name)
			}
		}

		timer := time.NewTimer(d.startPollInterval)
		select {
		case <-callCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, fmt.Errorf("herdr agent start %s: %w", name, callCtx.Err())
		case <-timer.C:
		}
	}
}

func (d *SocketDriver) PaneWaitOutput(ctx context.Context, paneID, match string, timeout time.Duration, regex bool) bool {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	matchType := "substring"
	if regex {
		matchType = "regex"
	}
	params := map[string]any{
		"pane_id": paneID,
		"source":  "recent",
		"match": map[string]any{
			"type":  matchType,
			"value": match,
		},
		"timeout_ms": durationMillis(timeout),
		"strip_ansi": true,
	}
	_, err := d.call(ctx, "pane.wait_for_output", params, timeoutWithGrace(timeout))
	return err == nil
}

func (d *SocketDriver) SendKeys(ctx context.Context, paneID string, keys []string) bool {
	_, err := d.call(ctx, "pane.send_keys", map[string]any{"pane_id": paneID, "keys": keys}, socketIOTimeout)
	return err == nil
}

func (d *SocketDriver) PaneRead(ctx context.Context, paneID string, lines int) (string, error) {
	if lines < 0 || uint64(lines) > math.MaxUint32 {
		return "", fmt.Errorf("herdr pane read %s: lines must fit uint32", paneID)
	}
	return d.paneReadText(ctx, paneID, map[string]any{
		"pane_id":    paneID,
		"source":     "recent",
		"format":     "text",
		"lines":      lines,
		"strip_ansi": true,
	})
}

// PaneScreen returns what the human is looking at. The visible source is the
// rendered screen including the harness composer; recent output is a scrollback
// slice and omitting lines is required because lines=0 reads nothing.
func (d *SocketDriver) PaneScreen(ctx context.Context, paneID string) (string, error) {
	return d.paneReadText(ctx, paneID, map[string]any{
		"pane_id":    paneID,
		"source":     "visible",
		"format":     "text",
		"strip_ansi": true,
	})
}

func (d *SocketDriver) paneReadText(ctx context.Context, paneID string, params map[string]any) (string, error) {
	raw, err := d.call(ctx, "pane.read", params, socketIOTimeout)
	if err != nil {
		return "", fmt.Errorf("herdr pane read %s: %w", paneID, err)
	}
	var response struct {
		Type string `json:"type"`
		Read *struct {
			Text string `json:"text"`
		} `json:"read"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "", fmt.Errorf("herdr pane read %s: %w", paneID, err)
	}
	if response.Type != "pane_read" || response.Read == nil {
		return "", fmt.Errorf("herdr pane read %s: unexpected %q response", paneID, response.Type)
	}
	return response.Read.Text, nil
}
