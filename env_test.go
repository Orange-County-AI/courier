package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mapLookup(values map[string]string) envLookup {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestEnvDualReadMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		values  map[string]string
		want    string
		wantSet bool
		wantErr string
	}{
		{name: "courier only", values: map[string]string{"COURIER_ORG": "new"}, want: "new", wantSet: true},
		{name: "channel only", values: map[string]string{"CHANNEL_ORG": "old"}, want: "old", wantSet: true},
		{name: "both same", values: map[string]string{"COURIER_ORG": "same", "CHANNEL_ORG": "same"}, want: "same", wantSet: true},
		{name: "both differ", values: map[string]string{"COURIER_ORG": "new", "CHANNEL_ORG": "old"}, wantErr: "COURIER_ORG and CHANNEL_ORG are both set and differ"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, set, err := env(mapLookup(test.values), "ORG")
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("env() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want || set != test.wantSet {
				t.Fatalf("env() = (%q, %t), want (%q, %t)", got, set, test.want, test.wantSet)
			}
		})
	}
}

func TestServeOptionsRejectDifferingDualNames(t *testing.T) {
	t.Parallel()
	_, err := serveOptionsFromEnv(mapLookup(map[string]string{
		"COURIER_ORG":    "new",
		"CHANNEL_ORG":    "old",
		"COURIER_TARGET": "agent",
	}), nil)
	if err == nil || !strings.Contains(err.Error(), "COURIER_ORG and CHANNEL_ORG") {
		t.Fatalf("serveOptionsFromEnv() error = %v", err)
	}
}

func TestEnvelopeTodoWarning(t *testing.T) {
	t.Parallel()
	var warnings []string
	_, err := serveOptionsFromEnv(mapLookup(map[string]string{
		"CHANNEL_ORG":           "org",
		"CHANNEL_TARGET":        "agent",
		"CHANNEL_ENVELOPE_TODO": "0",
	}), func(message string) {
		warnings = append(warnings, message)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "no longer has any effect") {
		t.Fatalf("warnings = %#v", warnings)
	}
}

func TestPreviewParsingIsTypoSafe(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		raw  string
		want bool
	}{
		{raw: "", want: true},
		{raw: "0", want: false},
		{raw: "false", want: false},
		{raw: " FALSE ", want: false},
		{raw: "no", want: true},
		{raw: "off", want: true},
		{raw: "typo", want: true},
		{raw: "1", want: true},
	} {
		if got := parsePreview(test.raw); got != test.want {
			t.Errorf("parsePreview(%q) = %t, want %t", test.raw, got, test.want)
		}
	}
}

func TestServeOptionsFullSurface(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"COURIER_ORG":                      "org",
		"COURIER_TARGET":                   "agent",
		"COURIER_DATA_DIR":                 "/data",
		"COURIER_DB_PATH":                  "/ledger.sqlite",
		"COURIER_PORT":                     "9000",
		"COURIER_BIND":                     "::1",
		"COURIER_SOCKET":                   "/tmp/courier.sock",
		"COURIER_PROMPT_TIMEOUT_MS":        "2500",
		"COURIER_TICK_MS":                  "3000",
		"COURIER_REDELIVER_GRACE_MS":       "4000",
		"COURIER_REDELIVER_MAX_BACKOFF_MS": "5000",
		"COURIER_REDELIVER_READ_FACTOR":    "2.5",
		"COURIER_CONNECTORS":               "mattermost, gmail, kaneo",
		"COURIER_SHADOW":                   "yes",
		"COURIER_SHADOW_HEARTBEAT_MS":      "6000",
		"COURIER_ENVELOPE_PREVIEW":         "false",
		"HERDR_SOCKET_PATH":                "/tmp/herdr.sock",
		"HERDR_SESSION":                    "session",
		"HERDR_BIN":                        "/bin/herdr",
		"MATTERMOST_URL":                   "https://chat.example",
		"MATTERMOST_BOT_TOKEN":             "token",
		"MATTERMOST_ATTACHMENT_DIR":        "/mm",
		"MATTERMOST_BOT_USER_ID":           "bot",
		"GMAIL_ACCOUNTS_JSON":              "[]",
		"GMAIL_ATTACHMENT_DIR":             "/gmail",
		"GMAIL_POLL_SECONDS":               "20",
		"KANEO_LISTEN_PORT":                "7788",
		"KANEO_CHANNEL_WEBHOOK_SECRET":     "secret",
		"KANEO_API_BASE":                   "https://kaneo.example",
		"KANEO_BOT_KEY":                    "key",
		"KANEO_WORKSPACE_ID":               "workspace",
		"KANEO_BOT_ACTOR":                  "bot",
	}
	opts, err := serveOptionsFromEnv(mapLookup(values), nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Org != "org" || opts.Target != "agent" || opts.DBPath != "/ledger.sqlite" || opts.Port != 9000 || opts.Bind != "::1" || opts.Socket != "/tmp/courier.sock" {
		t.Fatalf("serve identity/listen options = %#v", opts)
	}
	if opts.PromptTimeout != 2500*time.Millisecond || opts.TickInterval != 3*time.Second || opts.RedeliverGrace != 4*time.Second || opts.RedeliverMaxBackoff != 5*time.Second || opts.RedeliverReadFactor != 2.5 {
		t.Fatalf("serve timing options = %#v", opts)
	}
	if !reflect.DeepEqual(opts.Connectors, []string{"mattermost", "gmail", "kaneo"}) || !opts.Shadow.Enabled || opts.EnvelopePreview {
		t.Fatalf("serve mode options = %#v", opts)
	}
	if opts.Herdr.SocketPath != "/tmp/herdr.sock" || opts.Herdr.Session != "session" || opts.Herdr.Bin != "/bin/herdr" {
		t.Fatalf("herdr options = %#v", opts.Herdr)
	}
	if !opts.Mattermost.Enabled || opts.Mattermost.AttachmentDir != "/mm" || !opts.Gmail.Enabled || opts.Gmail.PollInterval != 20*time.Second || !opts.Kaneo.Enabled || opts.Kaneo.ListenPort != 7788 {
		t.Fatalf("connector options = %#v %#v %#v", opts.Mattermost, opts.Gmail, opts.Kaneo)
	}
}

func TestMCPOptionsDualReadAndDefaults(t *testing.T) {
	t.Parallel()
	opts, err := mcpOptionsFromEnv(mapLookup(map[string]string{
		"CHANNEL_AGENT": "agent",
		"HOME":          "/home/test",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if opts.Agent != "agent" || opts.HostURL != "http://127.0.0.1:8788" || opts.ManifestCacheDir != "/home/test/.cache/courier" {
		t.Fatalf("mcp options = %#v", opts)
	}
}

func TestRunVersionAndUnknownUsage(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if err := run([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got != courierVersion+"\n" {
		t.Fatalf("version output = %q", got)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"unknown"}, &stdout, &stderr); err == nil {
		t.Fatal("unknown subcommand succeeded")
	}
	if !strings.Contains(stderr.String(), "import  (reserved)") {
		t.Fatalf("usage = %q", stderr.String())
	}
}
