package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// push is the reference sender for spec/ingest-1.md: it signs exactly the bytes
// it transmits, so it doubles as an integrator's conformance smoke test and as
// the executable answer to "did I get the signature right?".
const pushUsage = `usage: courier push --source NAME [flags]

  --source NAME          declared ingest source (required)
  --content TEXT         message body; omit or pass - to read stdin
  --conversation ID      conversation_id; defaults to NAME:push
  --event-key KEY        idempotency key; defaults to a fresh uuid
  --user NAME            upstream display identity
  --trigger WORD         routing reason, e.g. mention or alert
  --meta KEY=VALUE       repeatable structured fact
  --url URL              ingest base URL; defaults to COURIER_INGEST_URL, then
                         http://127.0.0.1:$COURIER_INGEST_LISTEN_PORT

The source secret comes from COURIER_INGEST_SOURCES_JSON/_FILE when they declare
NAME, otherwise from COURIER_INGEST_SECRET. Secrets are never read from argv.`

type pushArgs struct {
	source       string
	content      string
	contentSet   bool
	conversation string
	eventKey     string
	user         string
	trigger      string
	url          string
	meta         map[string]string
}

func runPush(args []string, stdout io.Writer) error {
	parsed, err := parsePushArgs(args)
	if err != nil {
		return err
	}
	if !parsed.contentSet || parsed.content == "-" {
		piped, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read content from stdin: %w", err)
		}
		parsed.content = string(piped)
	}
	if strings.TrimSpace(parsed.content) == "" {
		return fmt.Errorf("content is required: pass --content TEXT or pipe it on stdin")
	}
	if parsed.eventKey == "" {
		generated, err := newID()
		if err != nil {
			return err
		}
		parsed.eventKey = generated
	}
	if parsed.conversation == "" {
		parsed.conversation = parsed.source + ":push"
	}
	secret, err := pushResolveSecret(os.LookupEnv, parsed.source)
	if err != nil {
		return err
	}
	base := parsed.url
	if base == "" {
		base, err = pushResolveURL(os.LookupEnv)
		if err != nil {
			return err
		}
	}

	body, err := json.Marshal(IngestEvent{
		Schema:         IngestSchema,
		EventKey:       parsed.eventKey,
		ConversationID: parsed.conversation,
		User:           parsed.user,
		Trigger:        parsed.trigger,
		Content:        parsed.content,
		Meta:           parsed.meta,
	})
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(base, "/") + IngestPathPrefix + parsed.source
	timestamp := time.Now().Unix()
	ctx, cancel := context.WithTimeout(context.Background(), clientRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(IngestTimestampHeader, strconv.FormatInt(timestamp, 10))
	request.Header.Set(IngestSignatureHeader, SignIngest(secret, timestamp, body))

	response, err := clientHTTP().Do(request)
	if err != nil {
		return fmt.Errorf("post to %s: %w", endpoint, err)
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	rendered := strings.TrimSpace(string(answer))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%s answered HTTP %d: %s", endpoint, response.StatusCode, rendered)
	}
	fmt.Fprintf(stdout, "HTTP %d %s\n", response.StatusCode, rendered)
	return nil
}

func parsePushArgs(args []string) (pushArgs, error) {
	parsed := pushArgs{meta: map[string]string{}}
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if flag == "--help" || flag == "-h" {
			return pushArgs{}, fmt.Errorf("%s", pushUsage)
		}
		name, inline, joined := strings.Cut(flag, "=")
		valueOf := func() (string, error) {
			if joined {
				return inline, nil
			}
			if index+1 >= len(args) {
				return "", fmt.Errorf("%s needs a value", name)
			}
			index++
			return args[index], nil
		}
		var value string
		var err error
		switch name {
		case "--source", "--content", "--conversation", "--event-key", "--user", "--trigger", "--url", "--meta":
			value, err = valueOf()
		default:
			return pushArgs{}, fmt.Errorf("unknown push flag %q\n%s", flag, pushUsage)
		}
		if err != nil {
			return pushArgs{}, err
		}
		switch name {
		case "--source":
			parsed.source = strings.TrimSpace(value)
		case "--content":
			parsed.content = value
			parsed.contentSet = true
		case "--conversation":
			parsed.conversation = strings.TrimSpace(value)
		case "--event-key":
			parsed.eventKey = strings.TrimSpace(value)
		case "--user":
			parsed.user = strings.TrimSpace(value)
		case "--trigger":
			parsed.trigger = strings.TrimSpace(value)
		case "--url":
			parsed.url = strings.TrimSpace(value)
		case "--meta":
			key, metaValue, ok := strings.Cut(value, "=")
			key = strings.TrimSpace(key)
			if !ok || key == "" {
				return pushArgs{}, fmt.Errorf("--meta takes KEY=VALUE, got %q", value)
			}
			parsed.meta[key] = metaValue
		}
	}
	if parsed.source == "" {
		return pushArgs{}, fmt.Errorf("--source is required\n%s", pushUsage)
	}
	if len(parsed.meta) == 0 {
		parsed.meta = nil
	}
	return parsed, nil
}

// pushResolveSecret prefers the operator's own declaration so the command signs
// with the same secret the daemon verifies with, and never accepts a secret on
// the command line.
func pushResolveSecret(lookup envLookup, source string) (string, error) {
	declared, active, err := LoadIngestSources(IngestOptions{
		Enabled:     true,
		SourcesJSON: envValue(lookup, "COURIER_INGEST_SOURCES_JSON"),
		SourcesFile: envValue(lookup, "COURIER_INGEST_SOURCES_FILE"),
	})
	if err == nil && active {
		for _, candidate := range declared {
			if candidate.Source == source {
				return candidate.Secret, nil
			}
		}
	}
	if secret := envValue(lookup, "COURIER_INGEST_SECRET"); secret != "" {
		return secret, nil
	}
	if err != nil {
		return "", fmt.Errorf("no secret for source %q: %w", source, err)
	}
	return "", fmt.Errorf(
		"no secret for source %q — declare it in COURIER_INGEST_SOURCES_JSON/_FILE or set COURIER_INGEST_SECRET",
		source,
	)
}

func pushResolveURL(lookup envLookup) (string, error) {
	if base := envValue(lookup, "COURIER_INGEST_URL"); base != "" {
		return base, nil
	}
	if port := envValue(lookup, "COURIER_INGEST_LISTEN_PORT"); port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return "", fmt.Errorf("COURIER_INGEST_LISTEN_PORT is not a port: %q", port)
		}
		return "http://127.0.0.1:" + port, nil
	}
	return "", fmt.Errorf("pass --url, or set COURIER_INGEST_URL or COURIER_INGEST_LISTEN_PORT")
}

func envValue(lookup envLookup, name string) string {
	raw, _ := lookup(name)
	return strings.TrimSpace(raw)
}
