package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const clientRequestTimeout = 30 * time.Second

// clientOptions is what the human-facing subcommands need to reach the daemon:
// its IPC base URL and nothing else. Unlike MCPOptions there is no agent
// identity here — kick, pause and inbox act as the operator, not as an agent.
type clientOptions struct {
	HostURL string
}

func loadClientOptions(lookup envLookup) (clientOptions, error) {
	hostURL, _, err := env(lookup, "HOST_URL")
	if err != nil {
		return clientOptions{}, err
	}
	hostURL = strings.TrimSpace(hostURL)
	if hostURL == "" {
		hostURL = fmt.Sprintf("http://%s:%d", defaultBind, defaultPort)
	}
	return clientOptions{HostURL: hostURL}, nil
}

func clientHTTP() *http.Client {
	return &http.Client{Timeout: clientRequestTimeout}
}

// clientGet and clientPost share the shim's transport so there is one HTTP
// client and one error vocabulary. shimDoJSON returns nil for a non-2xx whose
// body carries a readable text/error field, so every caller here checks the
// decoded `ok`/`error` itself rather than trusting the absent error.
func clientGet(ctx context.Context, opts clientOptions, path string, target any) error {
	return shimDoJSON(ctx, clientHTTP(), opts.HostURL, http.MethodGet, path, nil, target)
}

func clientPost(ctx context.Context, opts clientOptions, path string, body, target any) error {
	return shimDoJSON(ctx, clientHTTP(), opts.HostURL, http.MethodPost, path, body, target)
}

type clientHealth struct {
	OK            bool    `json:"ok"`
	Error         string  `json:"error"`
	Target        string  `json:"target"`
	Paused        bool    `json:"paused"`
	DraftHoldPane *string `json:"draft_hold_pane"`
}

type clientKick struct {
	OK       bool   `json:"ok"`
	Error    string `json:"error"`
	Busy     bool   `json:"busy"`
	Outcomes int    `json:"outcomes"`
}

type clientPause struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Paused bool   `json:"paused"`
}

type clientInbox struct {
	OK        bool       `json:"ok"`
	Error     string     `json:"error"`
	Target    string     `json:"target"`
	Paused    bool       `json:"paused"`
	DraftHold *DraftHold `json:"draft_hold"`
	Rows      []InboxRow `json:"rows"`
}

func clientFail(path, message string) error {
	if message == "" {
		message = "request failed"
	}
	return fmt.Errorf("courier %s: %s", path, message)
}

func clientFetchHealth(ctx context.Context, opts clientOptions) (clientHealth, error) {
	var health clientHealth
	if err := clientGet(ctx, opts, "/health", &health); err != nil {
		return clientHealth{}, err
	}
	if !health.OK {
		return clientHealth{}, clientFail("/health", health.Error)
	}
	return health, nil
}

func clientFetchInbox(ctx context.Context, opts clientOptions) (clientInbox, error) {
	var inbox clientInbox
	if err := clientGet(ctx, opts, "/inbox", &inbox); err != nil {
		return clientInbox{}, err
	}
	if !inbox.OK {
		return clientInbox{}, clientFail("/inbox", inbox.Error)
	}
	return inbox, nil
}

func clientKickNow(ctx context.Context, opts clientOptions) (clientKick, error) {
	var result clientKick
	if err := clientPost(ctx, opts, "/kick", nil, &result); err != nil {
		return clientKick{}, err
	}
	if !result.OK {
		return clientKick{}, clientFail("/kick", result.Error)
	}
	return result, nil
}

func clientSetPaused(ctx context.Context, opts clientOptions, paused bool) (clientPause, error) {
	var result clientPause
	if err := clientPost(ctx, opts, "/pause", map[string]bool{"paused": paused}, &result); err != nil {
		return clientPause{}, err
	}
	if !result.OK {
		return clientPause{}, clientFail("/pause", result.Error)
	}
	return result, nil
}

// runKick dispatches now. With --if-pane-matches it is a herdr event hook: it
// kicks only when the daemon's hold names the pane whose status just changed,
// and exits 0 in every other case so a per-session hook storm stays cheap and
// leaves no failed records in `herdr plugin log list`.
func runKick(args []string, stdout io.Writer) error {
	ifPaneMatches := false
	for _, arg := range args {
		switch arg {
		case "--if-pane-matches":
			ifPaneMatches = true
		default:
			return fmt.Errorf("unknown kick flag %q", arg)
		}
	}
	opts, err := loadClientOptions(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientRequestTimeout)
	defer cancel()

	if ifPaneMatches {
		pane, ok := kickEventPane(os.Getenv("HERDR_PLUGIN_EVENT_JSON"))
		if !ok {
			return nil
		}
		health, err := clientFetchHealth(ctx, opts)
		if err != nil {
			return nil
		}
		if health.DraftHoldPane == nil || *health.DraftHoldPane != pane {
			return nil
		}
	}

	result, err := clientKickNow(ctx, opts)
	if err != nil {
		if ifPaneMatches {
			return nil
		}
		return err
	}
	if result.Busy {
		fmt.Fprintln(stdout, "busy")
		return nil
	}
	fmt.Fprintf(stdout, "kicked (outcomes=%d)\n", result.Outcomes)
	return nil
}

// kickEventPane reads the pane id out of a herdr event hook payload. Anything
// unset or unparseable is "no pane", never an error: the hook must not become a
// source of noise.
func kickEventPane(raw string) (string, bool) {
	if strings.TrimSpace(raw) == "" {
		return "", false
	}
	var event struct {
		Data struct {
			PaneID string `json:"pane_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return "", false
	}
	if event.Data.PaneID == "" {
		return "", false
	}
	return event.Data.PaneID, true
}

func runPause(args []string, stdout io.Writer) error {
	mode := "toggle"
	for _, arg := range args {
		switch arg {
		case "--on":
			mode = "on"
		case "--off":
			mode = "off"
		case "--toggle":
			mode = "toggle"
		default:
			return fmt.Errorf("unknown pause flag %q", arg)
		}
	}
	opts, err := loadClientOptions(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientRequestTimeout)
	defer cancel()

	paused := mode == "on"
	if mode == "toggle" {
		health, err := clientFetchHealth(ctx, opts)
		if err != nil {
			return err
		}
		paused = !health.Paused
	}
	result, err := clientSetPaused(ctx, opts, paused)
	if err != nil {
		return err
	}
	if result.Paused {
		fmt.Fprintln(stdout, "paused")
		return nil
	}
	fmt.Fprintln(stdout, "resumed")
	return nil
}

// runPluginProbe is the plugin's startup hook. It always exits 0: a startup hook
// that fails does not stop the herdr server, and a non-zero exit only adds a
// failed record to the plugin log. An unreachable daemon is reported to the
// human as a notification instead.
func runPluginProbe(args []string, stdout, stderr io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("plugin-probe takes no arguments")
	}
	opts, err := loadClientOptions(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientRequestTimeout)
	defer cancel()
	if _, err := clientFetchHealth(ctx, opts); err != nil {
		fmt.Fprintf(stderr, "courier unreachable at %s: %v\n", opts.HostURL, err)
		probeNotify(ctx, fmt.Sprintf("%s: %v", opts.HostURL, err))
		return nil
	}
	fmt.Fprintf(stdout, "courier reachable at %s\n", opts.HostURL)
	return nil
}

// probeNotify goes through the herdr CLI rather than the socket: the probe runs
// as a plugin hook, where $HERDR_BIN_PATH is the sanctioned way to reach the
// server that started it.
func probeNotify(ctx context.Context, body string) {
	bin := os.Getenv("HERDR_BIN_PATH")
	if bin == "" {
		bin = "herdr"
	}
	command := exec.CommandContext(ctx, bin, "notification", "show", "courier: daemon unreachable", "--body", body)
	_ = command.Run()
}
