package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// inboxFakeDaemon serves the three plugin routes so the pane program is exercised
// over real HTTP, including the non-2xx-with-body quirk shimDoJSON has.
type inboxFakeDaemon struct {
	mu       sync.Mutex
	server   *httptest.Server
	inbox    clientInbox
	inboxErr int
	kick     clientKick
	kicks    int
	pauseSet []bool
}

func newInboxFakeDaemon(t *testing.T, inbox clientInbox) *inboxFakeDaemon {
	t.Helper()
	daemon := &inboxFakeDaemon{inbox: inbox, kick: clientKick{OK: true}}
	daemon.inbox.OK = true
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "target": daemon.inbox.Target, "paused": daemon.inbox.Paused,
		})
	})
	mux.HandleFunc("GET /inbox", func(w http.ResponseWriter, _ *http.Request) {
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		if daemon.inboxErr != 0 {
			writeJSON(w, daemon.inboxErr, map[string]any{"error": "inbox exploded"})
			return
		}
		writeJSON(w, http.StatusOK, daemon.inbox)
	})
	mux.HandleFunc("POST /kick", func(w http.ResponseWriter, _ *http.Request) {
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		daemon.kicks++
		writeJSON(w, http.StatusOK, daemon.kick)
	})
	mux.HandleFunc("POST /pause", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Paused bool `json:"paused"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "paused must be a boolean"})
			return
		}
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		daemon.pauseSet = append(daemon.pauseSet, body.Paused)
		daemon.inbox.Paused = body.Paused
		writeJSON(w, http.StatusOK, clientPause{OK: true, Paused: body.Paused})
	})
	daemon.server = httptest.NewServer(mux)
	t.Cleanup(daemon.server.Close)
	return daemon
}

func (d *inboxFakeDaemon) opts() clientOptions {
	return clientOptions{HostURL: d.server.URL}
}

func (d *inboxFakeDaemon) kickCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.kicks
}

func (d *inboxFakeDaemon) pauseLog() []bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]bool(nil), d.pauseSet...)
}

func inboxRun(t *testing.T, daemon *inboxFakeDaemon, input string) string {
	t.Helper()
	var out strings.Builder
	if err := runInbox(context.Background(), daemon.opts(), strings.NewReader(input), &out); err != nil {
		t.Fatalf("runInbox: %v", err)
	}
	return out.String()
}

func inboxHeldRow() clientInbox {
	return clientInbox{
		Target:    "my-agent",
		DraftHold: &DraftHold{PaneID: "w7V:p1", Agent: "omp", At: 1786943678814},
		Rows: []InboxRow{{
			DeliveryID: "d-1", EventID: 12, Status: "pending", Connector: "mattermost",
			User: "Dana", CreatedAt: 1, Preview: "Can you check the batch",
		}},
	}
}

func TestInboxRendersEmptyQueueAndQuits(t *testing.T) {
	daemon := newInboxFakeDaemon(t, clientInbox{Target: "my-agent"})
	frame := inboxRun(t, daemon, "q\n")
	if !strings.Contains(frame, "courier inbox — my-agent — delivering") {
		t.Fatalf("header missing: %q", frame)
	}
	if !strings.Contains(frame, "no messages waiting") {
		t.Fatalf("empty queue line missing: %q", frame)
	}
	if !strings.Contains(frame, "[enter] refresh   [d] deliver now   [p] pause/resume   [q] quit") {
		t.Fatalf("footer missing: %q", frame)
	}
	if daemon.kickCount() != 0 {
		t.Fatal("listing the inbox kicked the daemon")
	}
}

func TestInboxRendersHeldRowAndReasonAfterKick(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	frame := inboxRun(t, daemon, "d\nq\n")
	if !strings.Contains(frame, "courier inbox — my-agent — held: your composer has unsent input (omp, pane w7V:p1)") {
		t.Fatalf("held header missing: %q", frame)
	}
	if !strings.Contains(frame, "mattermost") || !strings.Contains(frame, "Dana") ||
		!strings.Contains(frame, "pending") || !strings.Contains(frame, "Can you check the batch") {
		t.Fatalf("row missing: %q", frame)
	}
	if !strings.Contains(frame, "still held — clear your composer, then it delivers on its own") {
		t.Fatalf("held status missing: %q", frame)
	}
	if daemon.kickCount() != 1 {
		t.Fatalf("kicks = %d, want 1", daemon.kickCount())
	}
}

func TestInboxReportsDeliveredCountWithoutAHold(t *testing.T) {
	inbox := inboxHeldRow()
	inbox.DraftHold = nil
	daemon := newInboxFakeDaemon(t, inbox)
	daemon.kick = clientKick{OK: true, Outcomes: 1}
	if frame := inboxRun(t, daemon, "d\nq\n"); !strings.Contains(frame, "delivered 1") {
		t.Fatalf("delivered status missing: %q", frame)
	}
}

func TestInboxReportsNothingToDeliverAndBusy(t *testing.T) {
	inbox := inboxHeldRow()
	inbox.DraftHold = nil
	inbox.Rows = nil
	daemon := newInboxFakeDaemon(t, inbox)
	if frame := inboxRun(t, daemon, "d\nq\n"); !strings.Contains(frame, "nothing to deliver") {
		t.Fatalf("idle status missing: %q", frame)
	}
	daemon.kick = clientKick{OK: true, Busy: true}
	if frame := inboxRun(t, daemon, "d\nq\n"); !strings.Contains(frame, "busy — another delivery is in flight") {
		t.Fatalf("busy status missing: %q", frame)
	}
}

func TestInboxPauseSendsTheInverseAndShowsPaused(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	frame := inboxRun(t, daemon, "p\nq\n")
	if want := []bool{true}; len(daemon.pauseLog()) != 1 || daemon.pauseLog()[0] != want[0] {
		t.Fatalf("pause calls = %#v, want the inverse of the served paused value", daemon.pauseLog())
	}
	if !strings.Contains(frame, "  [paused]") {
		t.Fatalf("paused header missing on the next frame: %q", frame)
	}
	if !strings.Contains(frame, "\npaused\n") {
		t.Fatalf("paused status missing: %q", frame)
	}
}

func TestInboxUnknownCommandAndDaemonErrorKeepThePaneAlive(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	if frame := inboxRun(t, daemon, "x\nq\n"); !strings.Contains(frame, "unknown command: x") {
		t.Fatalf("unknown command status missing: %q", frame)
	}

	daemon.inboxErr = http.StatusInternalServerError
	frame := inboxRun(t, daemon, "q\n")
	if !strings.Contains(frame, "error: ") {
		t.Fatalf("error status missing: %q", frame)
	}
	if !strings.Contains(frame, "no messages waiting") {
		t.Fatalf("errored frame should still render a body: %q", frame)
	}
}

func TestInboxEOFExitsWithoutACommand(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	if frame := inboxRun(t, daemon, ""); !strings.Contains(frame, "courier inbox — my-agent") {
		t.Fatalf("EOF frame missing: %q", frame)
	}
}

func TestInboxAgeUsesLargestWholeUnit(t *testing.T) {
	for _, test := range []struct {
		ms   int64
		want string
	}{{-5, "0s"}, {45_000, "45s"}, {59_999, "59s"}, {12 * 60_000, "12m"}, {3 * 3_600_000, "3h"}, {2 * 86_400_000, "2d"}} {
		if got := inboxAge(test.ms); got != test.want {
			t.Errorf("inboxAge(%d) = %q, want %q", test.ms, got, test.want)
		}
	}
}

func TestInboxClipsTheMessageToTheWidth(t *testing.T) {
	line := inboxRowLine("1", "2m", "mattermost", "Dana", "pending", strings.Repeat("x", 200), 60)
	if runes := len([]rune(line)); runes != 60 {
		t.Fatalf("line width = %d, want 60: %q", runes, line)
	}
	if !strings.HasSuffix(line, "…") {
		t.Fatalf("truncation mark missing: %q", line)
	}
}

func TestKickEventPaneReadsHerdrHookPayload(t *testing.T) {
	payload := `{"event":"pane_agent_status_changed","data":{"type":"pane_agent_status_changed",` +
		`"pane_id":"w7V:p1","workspace_id":"w7V","agent_status":"working"}}`
	if pane, ok := kickEventPane(payload); !ok || pane != "w7V:p1" {
		t.Fatalf("kickEventPane = %q, %t", pane, ok)
	}
	for _, raw := range []string{"", "   ", "not json", `{"data":{}}`, `{"data":{"pane_id":""}}`} {
		if pane, ok := kickEventPane(raw); ok {
			t.Errorf("kickEventPane(%q) = %q, %t, want no pane", raw, pane, ok)
		}
	}
}

func TestKickIfPaneMatchesOnlyKicksTheHeldPane(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	held := "w7V:p1"
	hold := &held
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "draft_hold_pane": hold})
	})
	mux.HandleFunc("POST /kick", func(w http.ResponseWriter, _ *http.Request) {
		daemon.mu.Lock()
		daemon.kicks++
		daemon.mu.Unlock()
		writeJSON(w, http.StatusOK, clientKick{OK: true, Outcomes: 1})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	t.Setenv("COURIER_HOST_URL", server.URL)

	event := func(pane string) string {
		return `{"event":"pane_agent_status_changed","data":{"pane_id":"` + pane + `","agent_status":"working"}}`
	}
	var out strings.Builder

	t.Setenv("HERDR_PLUGIN_EVENT_JSON", event("w7V:p1"))
	if err := runKick([]string{"--if-pane-matches"}, &out); err != nil {
		t.Fatalf("matching kick: %v", err)
	}
	if daemon.kickCount() != 1 {
		t.Fatalf("kicks = %d, want the held pane to kick", daemon.kickCount())
	}

	t.Setenv("HERDR_PLUGIN_EVENT_JSON", event("w7V:p9"))
	if err := runKick([]string{"--if-pane-matches"}, &out); err != nil {
		t.Fatalf("other pane kick: %v", err)
	}
	if daemon.kickCount() != 1 {
		t.Fatalf("kicks = %d, want no kick for another pane", daemon.kickCount())
	}

	hold = nil
	t.Setenv("HERDR_PLUGIN_EVENT_JSON", event("w7V:p1"))
	if err := runKick([]string{"--if-pane-matches"}, &out); err != nil {
		t.Fatalf("unheld kick: %v", err)
	}
	if daemon.kickCount() != 1 {
		t.Fatalf("kicks = %d, want no kick with a null hold", daemon.kickCount())
	}

	// An unset payload is a silent success: hooks must never be noisy.
	t.Setenv("HERDR_PLUGIN_EVENT_JSON", "")
	if err := runKick([]string{"--if-pane-matches"}, &out); err != nil {
		t.Fatalf("empty payload kick: %v", err)
	}
}

func TestKickAndPausePrintTheirResult(t *testing.T) {
	daemon := newInboxFakeDaemon(t, inboxHeldRow())
	daemon.kick = clientKick{OK: true, Outcomes: 2}
	t.Setenv("COURIER_HOST_URL", daemon.server.URL)

	var out strings.Builder
	if err := runKick(nil, &out); err != nil {
		t.Fatalf("runKick: %v", err)
	}
	if got := out.String(); got != "kicked (outcomes=2)\n" {
		t.Fatalf("kick output = %q", got)
	}

	out.Reset()
	if err := runPause([]string{"--toggle"}, &out); err != nil {
		t.Fatalf("runPause: %v", err)
	}
	if got := out.String(); got != "paused\n" {
		t.Fatalf("pause output = %q", got)
	}
	out.Reset()
	if err := runPause([]string{"--toggle"}, &out); err != nil {
		t.Fatalf("runPause resume: %v", err)
	}
	if got := out.String(); got != "resumed\n" {
		t.Fatalf("resume output = %q", got)
	}
	if pauses := daemon.pauseLog(); len(pauses) != 2 || !pauses[0] || pauses[1] {
		t.Fatalf("pause calls = %#v, want a toggle each way", pauses)
	}
}

func TestClientOptionsDefaultToTheDaemonBind(t *testing.T) {
	opts, err := loadClientOptions(func(string) (string, bool) { return "", false })
	if err != nil || opts.HostURL != "http://127.0.0.1:8788" {
		t.Fatalf("loadClientOptions = %#v, %v", opts, err)
	}
	opts, err = loadClientOptions(func(name string) (string, bool) {
		if name == "CHANNEL_HOST_URL" {
			return " http://127.0.0.1:8799 ", true
		}
		return "", false
	})
	if err != nil || opts.HostURL != "http://127.0.0.1:8799" {
		t.Fatalf("CHANNEL_HOST_URL = %#v, %v", opts, err)
	}
	if _, err := loadClientOptions(func(name string) (string, bool) {
		switch name {
		case "COURIER_HOST_URL":
			return "http://a", true
		case "CHANNEL_HOST_URL":
			return "http://b", true
		}
		return "", false
	}); err == nil {
		t.Fatal("conflicting COURIER_/CHANNEL_ host URLs were accepted")
	}
}
