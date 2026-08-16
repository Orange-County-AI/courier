package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReply struct {
	result     any
	apiError   *herdrAPIError
	drop       bool
	noResponse bool
}

type fakeSocketServer struct {
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

func startFakeSocketServer(t *testing.T, protocol int, handler func(wireRequest) fakeReply) *fakeSocketServer {
	t.Helper()
	return startFakeSocketServerWithProtocol(t, func() int { return protocol }, handler)
}

func startFakeSocketServerWithProtocol(t *testing.T, protocol func() int, handler func(wireRequest) fakeReply) *fakeSocketServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeSocketServer{listener: listener, done: make(chan struct{})}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-server.done:
					return
				default:
					t.Errorf("fake herdr accept: %v", err)
					return
				}
			}
			server.wg.Add(1)
			go server.serveConn(t, conn, protocol, handler)
		}
	}()
	t.Cleanup(func() {
		close(server.done)
		_ = listener.Close()
		server.wg.Wait()
	})
	return server
}

func (s *fakeSocketServer) serveConn(t *testing.T, conn net.Conn, protocol func() int, handler func(wireRequest) fakeReply) {
	defer s.wg.Done()
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var request wireRequest
	if err := decoder.Decode(&request); err != nil {
		if !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "EOF") {
			t.Errorf("fake herdr decode: %v", err)
		}
		return
	}
	if request.Method == "ping" {
		_ = encoder.Encode(wireResponse{
			ID: request.ID,
			Result: mustJSON(t, map[string]any{
				"type":     "pong",
				"version":  "0.8.0",
				"protocol": protocol(),
			}),
		})
		return
	}

	reply := handler(request)
	if reply.drop {
		return
	}
	if reply.noResponse {
		<-s.done
		return
	}
	response := wireResponse{ID: request.ID, Error: reply.apiError}
	if reply.apiError == nil {
		response.Result = mustJSON(t, reply.result)
	}
	_ = encoder.Encode(response)
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newTestDriver(t *testing.T, handler func(wireRequest) fakeReply) *SocketDriver {
	t.Helper()
	server := startFakeSocketServer(t, HerdrProtocolVersion, handler)
	driver, err := NewSocketDriver(context.Background(), SocketDriverOptions{SocketPath: server.listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	return driver
}

func apiFailure(code, message string) fakeReply {
	return fakeReply{apiError: &herdrAPIError{Code: code, Message: message}}
}

func agentResult(status, paneID string, seq uint64) map[string]any {
	return map[string]any{
		"type": "agent_info",
		"agent": map[string]any{
			"name":             "agent-a",
			"agent":            "omp",
			"agent_status":     status,
			"pane_id":          paneID,
			"terminal_id":      "term-1",
			"state_change_seq": seq,
		},
	}
}

// These nine tests port the TypeScript predecessor's herdr tests. The transport fake is
// now a real in-process Unix socket, so framing and disconnect behavior are
// exercised instead of replacing the driver's process runner.
func TestGetAgentNotFoundReturnsNil(t *testing.T) {
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		if request.Method != "agent.get" {
			t.Errorf("unexpected method %s", request.Method)
		}
		return apiFailure("agent_not_found", "no agent named agent-a")
	})
	agent, err := driver.GetAgent(context.Background(), "agent-a")
	if err != nil || agent != nil {
		t.Fatalf("GetAgent = (%v, %v), want (nil, nil)", agent, err)
	}
}

func TestGetAgentOtherErrorNamesCause(t *testing.T) {
	driver := newTestDriver(t, func(wireRequest) fakeReply {
		return apiFailure("agent_not_ready", "pane is not at a prompt")
	})
	_, err := driver.GetAgent(context.Background(), "agent-a")
	if err == nil || !strings.Contains(err.Error(), "pane is not at a prompt") {
		t.Fatalf("GetAgent error = %v", err)
	}
	if strings.Contains(err.Error(), "{") {
		t.Fatalf("GetAgent exposed raw error JSON: %v", err)
	}
}

func TestStartAgentSurfacesAPIMessage(t *testing.T) {
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		if request.Method != "agent.start" {
			t.Errorf("unexpected method %s", request.Method)
		}
		return apiFailure("pane_busy", "pane w1:p1 is running a command")
	})
	_, err := driver.StartAgent(context.Background(), "agent-a", "omp", "w1:p1", nil)
	if err == nil || !strings.Contains(err.Error(), "pane w1:p1 is running a command") {
		t.Fatalf("StartAgent error = %v", err)
	}
	if strings.Contains(err.Error(), "{") {
		t.Fatalf("StartAgent exposed raw error JSON: %v", err)
	}
}

func TestStartAgentWaitsForInteractiveAgent(t *testing.T) {
	getCount := 0
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		switch request.Method {
		case "agent.start":
			result := agentResult("idle", "w1:p1", 1)
			result["type"] = "agent_started"
			agent := result["agent"].(map[string]any)
			agent["launch_pending"] = true
			return fakeReply{result: result}
		case "agent.get":
			getCount++
			result := agentResult("idle", "w1:p1", uint64(1+getCount))
			agent := result["agent"].(map[string]any)
			agent["interactive_ready"] = true
			return fakeReply{result: result}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{result: map[string]any{"type": "ok"}}
		}
	})
	driver.startPollInterval = time.Millisecond
	agent, err := driver.StartAgent(context.Background(), "agent-a", "omp", "w1:p1", []string{"--resume=session-1"})
	if err != nil || agent == nil || !agent.InteractiveReady {
		t.Fatalf("StartAgent = (%v, %v), want interactive agent", agent, err)
	}
}

func TestPromptStallWithoutPaneReturnsOriginalStall(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		switch request.Method {
		case "agent.prompt":
			return apiFailure("agent_prompt_stalled", "no state change within 5000ms")
		case "agent.get":
			return fakeReply{result: agentResult("idle", "", 7)}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})
	result := driver.PromptAgent(context.Background(), "agent-a", "hello", time.Second)
	if result.OK || result.Code != "agent_prompt_stalled" || !strings.Contains(result.Error, "no state change") {
		t.Fatalf("PromptAgent = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, method := range methods {
		if method == "pane.send_keys" {
			t.Fatal("stall recovery sent Enter without a pane")
		}
	}
}

func TestPromptStallSendsOneEnterThenWaits(t *testing.T) {
	var mu sync.Mutex
	getCount := 0
	var methods []string
	var sentKeys []string
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		mu.Lock()
		defer mu.Unlock()
		methods = append(methods, request.Method)
		switch request.Method {
		case "agent.prompt":
			return apiFailure("agent_prompt_stalled", "no state change within 5000ms")
		case "agent.get":
			getCount++
			seq := uint64(7)
			if getCount > 1 {
				seq = 8
			}
			return fakeReply{result: agentResult("idle", "w1:p1", seq)}
		case "pane.send_keys":
			params := request.Params.(map[string]any)
			for _, key := range params["keys"].([]any) {
				sentKeys = append(sentKeys, key.(string))
			}
			return fakeReply{result: map[string]any{"type": "ok"}}
		case "agent.wait":
			return fakeReply{result: agentResult("idle", "w1:p1", 9)}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})
	driver.stallPollInterval = time.Millisecond
	result := driver.PromptAgent(context.Background(), "agent-a", "a 169-line envelope", time.Second)
	if !result.OK || result.Blocked || result.Code != "" {
		t.Fatalf("PromptAgent = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sentKeys) != 1 || sentKeys[0] != "Enter" {
		t.Fatalf("sent keys = %v, want [Enter]", sentKeys)
	}
	if countMethod(methods, "agent.prompt") != 1 || countMethod(methods, "agent.wait") != 1 {
		t.Fatalf("methods = %v", methods)
	}
	if indexMethod(methods, "pane.send_keys") > indexMethod(methods, "agent.wait") {
		t.Fatalf("wait happened before Enter: %v", methods)
	}
}

func TestPromptStallSettlesBlocked(t *testing.T) {
	getCount := 0
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		switch request.Method {
		case "agent.prompt":
			return apiFailure("agent_prompt_stalled", "stalled")
		case "agent.get":
			getCount++
			return fakeReply{result: agentResult("idle", "w1:p1", uint64(6+getCount))}
		case "pane.send_keys":
			return fakeReply{result: map[string]any{"type": "ok"}}
		case "agent.wait":
			return fakeReply{result: agentResult("blocked", "w1:p1", 10)}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})
	driver.stallPollInterval = time.Millisecond
	result := driver.PromptAgent(context.Background(), "agent-a", "a 169-line envelope", time.Second)
	if !result.OK || !result.Blocked {
		t.Fatalf("PromptAgent = %+v, want ok blocked", result)
	}
}

func TestPromptStallFrozenSequenceReturnsOriginalStall(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		switch request.Method {
		case "agent.prompt":
			return apiFailure("agent_prompt_stalled", "no state change within 5000ms")
		case "agent.get":
			return fakeReply{result: agentResult("idle", "w1:p1", 7)}
		case "pane.send_keys":
			return fakeReply{result: map[string]any{"type": "ok"}}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})
	driver.stallPollInterval = time.Millisecond
	result := driver.PromptAgent(context.Background(), "agent-a", "a 169-line envelope", time.Second)
	if result.OK || result.Code != "agent_prompt_stalled" {
		t.Fatalf("PromptAgent = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := countMethod(methods, "agent.get"); got != 16 {
		t.Fatalf("agent.get count = %d, want pre-read + 15 polls", got)
	}
	if countMethod(methods, "agent.wait") != 0 {
		t.Fatalf("waited without sequence movement: %v", methods)
	}
}

func TestPromptStallWaitTimeoutNeverReportsOK(t *testing.T) {
	getCount := 0
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		switch request.Method {
		case "agent.prompt":
			return apiFailure("agent_prompt_stalled", "stalled")
		case "agent.get":
			getCount++
			return fakeReply{result: agentResult("idle", "w1:p1", uint64(7+getCount))}
		case "pane.send_keys":
			return fakeReply{result: map[string]any{"type": "ok"}}
		case "agent.wait":
			return apiFailure("timeout", "agent did not settle within 120000ms")
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})
	driver.stallPollInterval = time.Millisecond
	result := driver.PromptAgent(context.Background(), "agent-a", "a 169-line envelope", time.Second)
	if result.OK || result.Code != "timeout" {
		t.Fatalf("PromptAgent = %+v", result)
	}
}

func TestPromptFlushOnlyAttemptedForStall(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		return apiFailure("agent_not_found", "no agent named agent-a")
	})
	result := driver.PromptAgent(context.Background(), "agent-a", "hello", time.Second)
	if result.Code != "agent_not_found" {
		t.Fatalf("PromptAgent = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != "agent.prompt" {
		t.Fatalf("methods = %v, want only agent.prompt", methods)
	}
}

func TestPromptDirectBlockedIsNotAnswer(t *testing.T) {
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		if request.Method != "agent.prompt" {
			t.Errorf("unexpected method %s", request.Method)
		}
		result := agentResult("blocked", "w1:p1", 3)
		result["type"] = "agent_prompted"
		return fakeReply{result: result}
	})
	result := driver.PromptAgent(context.Background(), "agent-a", "hello", time.Second)
	if !result.OK || !result.Blocked {
		t.Fatalf("PromptAgent = %+v, want consumed but blocked", result)
	}
}

func TestFirstUsePingProtocolAllowlist(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
		server := startFakeSocketServer(t, HerdrProtocolVersion, func(wireRequest) fakeReply {
			return apiFailure("agent_not_found", "no agent named agent-a")
		})
		var logs []string
		driver, err := NewSocketDriver(context.Background(), SocketDriverOptions{
			SocketPath: server.listener.Addr().String(),
			Log:        func(message string) { logs = append(logs, message) },
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = driver.Close() })
		if len(logs) != 0 {
			t.Fatalf("constructor logs = %v, want no I/O evidence", logs)
		}
		if agent, err := driver.GetAgent(context.Background(), "agent-a"); err != nil || agent != nil {
			t.Fatalf("GetAgent = (%v, %v), want (nil, nil)", agent, err)
		}
		if len(logs) != 1 || !strings.Contains(logs[0], "protocol 19 accepted") {
			t.Fatalf("first-use logs = %v", logs)
		}
	})

	t.Run("refused unknown", func(t *testing.T) {
		t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
		server := startFakeSocketServer(t, HerdrProtocolVersion+1, func(wireRequest) fakeReply {
			t.Error("protocol refusal allowed a non-ping request")
			return fakeReply{}
		})
		driver, err := NewSocketDriver(context.Background(), SocketDriverOptions{
			SocketPath: server.listener.Addr().String(),
			Log:        func(string) {},
		})
		if err != nil {
			t.Fatalf("lazy constructor returned %v", err)
		}
		t.Cleanup(func() { _ = driver.Close() })
		_, err = driver.GetAgent(context.Background(), "agent-a")
		if err == nil || !strings.Contains(err.Error(), "server 20") || !strings.Contains(err.Error(), "accepts [19]") {
			t.Fatalf("GetAgent error = %v", err)
		}
	})

	t.Run("allowed via env", func(t *testing.T) {
		t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "20")
		server := startFakeSocketServer(t, HerdrProtocolVersion+1, func(wireRequest) fakeReply {
			return apiFailure("agent_not_found", "no agent named agent-a")
		})
		var logs []string
		driver, err := NewSocketDriver(context.Background(), SocketDriverOptions{
			SocketPath: server.listener.Addr().String(),
			Log:        func(message string) { logs = append(logs, message) },
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = driver.Close() })
		if agent, err := driver.GetAgent(context.Background(), "agent-a"); err != nil || agent != nil {
			t.Fatalf("GetAgent = (%v, %v), want (nil, nil)", agent, err)
		}
		if len(logs) != 1 || !strings.Contains(logs[0], "protocol 20 accepted") {
			t.Fatalf("first-use logs = %v", logs)
		}
	})
}

func TestConcurrentPromptAndGetAreFramedSafely(t *testing.T) {
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		switch request.Method {
		case "agent.prompt":
			time.Sleep(5 * time.Millisecond)
			result := agentResult("idle", "w1:p1", 4)
			result["type"] = "agent_prompted"
			return fakeReply{result: result}
		case "agent.get":
			return fakeReply{result: agentResult("idle", "w1:p1", 4)}
		default:
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{}
		}
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var prompt PromptResult
	var agent *Agent
	var getErr error
	go func() {
		defer wg.Done()
		<-start
		prompt = driver.PromptAgent(context.Background(), "agent-a", "hello", time.Second)
	}()
	go func() {
		defer wg.Done()
		<-start
		agent, getErr = driver.GetAgent(context.Background(), "agent-a")
	}()
	close(start)
	wg.Wait()
	if !prompt.OK || getErr != nil || agent == nil {
		t.Fatalf("PromptAgent = %+v; GetAgent = (%v, %v)", prompt, agent, getErr)
	}
}

func TestDroppedConnectionFailsRequestThenReconnects(t *testing.T) {
	var mu sync.Mutex
	getCount := 0
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		if request.Method != "agent.get" {
			t.Errorf("unexpected method %s", request.Method)
			return fakeReply{result: map[string]any{"type": "ok"}}
		}
		mu.Lock()
		defer mu.Unlock()
		getCount++
		if getCount == 1 {
			return fakeReply{drop: true}
		}
		return fakeReply{result: agentResult("idle", "w1:p1", 2)}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := driver.GetAgent(ctx, "agent-a")
	if err == nil {
		t.Fatal("GetAgent succeeded after connection drop")
	}
	if time.Since(started) >= 200*time.Millisecond {
		t.Fatalf("connection drop waited for context timeout: %v", time.Since(started))
	}
	agent, err := driver.GetAgent(context.Background(), "agent-a")
	if err != nil || agent == nil {
		t.Fatalf("GetAgent after reconnect = (%v, %v)", agent, err)
	}
}

func TestReconnectRepeatsProtocolValidation(t *testing.T) {
	t.Setenv("COURIER_HERDR_PROTOCOL_ALLOW", "")
	var protocol atomic.Int64
	protocol.Store(HerdrProtocolVersion)
	server := startFakeSocketServerWithProtocol(t, func() int {
		return int(protocol.Load())
	}, func(request wireRequest) fakeReply {
		if request.Method != "agent.get" {
			t.Errorf("unexpected method %s", request.Method)
		}
		protocol.Store(HerdrProtocolVersion + 1)
		return fakeReply{drop: true}
	})
	driver, err := NewSocketDriver(context.Background(), SocketDriverOptions{
		SocketPath: server.listener.Addr().String(),
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })

	if _, err := driver.GetAgent(context.Background(), "agent-a"); err == nil {
		t.Fatal("first GetAgent succeeded after connection drop")
	}
	if _, err := driver.GetAgent(context.Background(), "agent-a"); err == nil ||
		!strings.Contains(err.Error(), "server 20") ||
		!strings.Contains(err.Error(), "accepts [19]") {
		t.Fatalf("GetAgent after reconnect error = %v", err)
	}
}

func TestNoResponseHonorsCallerDeadline(t *testing.T) {
	driver := newTestDriver(t, func(request wireRequest) fakeReply {
		if request.Method != "agent.prompt" {
			t.Errorf("unexpected method %s", request.Method)
		}
		return fakeReply{noResponse: true}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result := driver.PromptAgent(ctx, "agent-a", "hello", time.Second)
	if result.OK || result.Error == "" {
		t.Fatalf("PromptAgent = %+v", result)
	}
}

func TestListAgentsKeepsEmptyDistinctFromFailure(t *testing.T) {
	t.Run("empty is fact", func(t *testing.T) {
		driver := newTestDriver(t, func(request wireRequest) fakeReply {
			return fakeReply{result: map[string]any{"type": "agent_list", "agents": []any{}}}
		})
		agents, err := driver.ListAgents(context.Background())
		if err != nil || agents == nil || len(agents) != 0 {
			t.Fatalf("ListAgents = (%v, %v), want non-nil empty slice", agents, err)
		}
	})
	t.Run("failure is error", func(t *testing.T) {
		driver := newTestDriver(t, func(request wireRequest) fakeReply {
			return apiFailure("permission_denied", "not allowed")
		})
		agents, err := driver.ListAgents(context.Background())
		if err == nil || agents != nil {
			t.Fatalf("ListAgents = (%v, %v), want error", agents, err)
		}
	})
}

func TestSocketPathResolutionPrecedence(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HERDR_SOCKET_PATH", "/env/override.sock")
	t.Setenv("HERDR_SESSION", "from-env")

	path, err := resolveHerdrSocketPath(SocketDriverOptions{SocketPath: "/explicit.sock", Session: "explicit-session"})
	if err != nil || path != "/explicit.sock" {
		t.Fatalf("explicit path = %q, %v", path, err)
	}
	path, err = resolveHerdrSocketPath(SocketDriverOptions{Session: "explicit-session"})
	if err != nil || path != filepath.Join(xdg, "herdr", "sessions", "explicit-session", "herdr.sock") {
		t.Fatalf("explicit session = %q, %v", path, err)
	}
	path, err = resolveHerdrSocketPath(SocketDriverOptions{})
	if err != nil || path != "/env/override.sock" {
		t.Fatalf("env override = %q, %v", path, err)
	}
	t.Setenv("HERDR_SOCKET_PATH", "")
	path, err = resolveHerdrSocketPath(SocketDriverOptions{})
	if err != nil || path != filepath.Join(xdg, "herdr", "sessions", "from-env", "herdr.sock") {
		t.Fatalf("named env session = %q, %v", path, err)
	}
	t.Setenv("HERDR_SESSION", "")
	path, err = resolveHerdrSocketPath(SocketDriverOptions{})
	if err != nil || path != filepath.Join(xdg, "herdr", "herdr.sock") {
		t.Fatalf("default socket = %q, %v", path, err)
	}
}

func countMethod(methods []string, want string) int {
	count := 0
	for _, method := range methods {
		if method == want {
			count++
		}
	}
	return count
}

func indexMethod(methods []string, want string) int {
	for index, method := range methods {
		if method == want {
			return index
		}
	}
	return -1
}
