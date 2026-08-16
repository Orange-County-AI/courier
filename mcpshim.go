package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	shimManifestRetries   = 20
	shimManifestRetryWait = 500 * time.Millisecond
)

var shimCacheNamePattern = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

type shimLogFunc func(string, ...any)

type shimSleepFunc func(context.Context, time.Duration) error

type shimManifestLoadOptions struct {
	HostURL  string
	CacheDir string
	Client   *http.Client
	Sleep    shimSleepFunc
	Log      shimLogFunc
}

type shimToolResponse struct {
	Text    string `json:"text"`
	IsError bool   `json:"is_error"`
	Error   string `json:"error"`
}

func shimDefaultSleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shimCachePath(cacheDir, hostURL string) string {
	name := shimCacheNamePattern.ReplaceAllString(hostURL, "_")
	return filepath.Join(cacheDir, name+".json")
}

func shimDoJSON(ctx context.Context, client *http.Client, hostURL, method, path string, body, target any) error {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode host %s %s body: %w", method, path, err)
		}
		requestBody = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(hostURL, "/")+path, requestBody)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read host %s %s response: %w", method, path, err)
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if err := json.Unmarshal(payload, target); err != nil {
		text := string(payload)
		if len(text) > 400 {
			text = text[:400]
		}
		return fmt.Errorf("host %s %s: HTTP %d: %s", method, path, response.StatusCode, text)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var readable shimToolResponse
		if err := json.Unmarshal(payload, &readable); err == nil && (readable.Text != "" || readable.Error != "") {
			return nil
		}
		return fmt.Errorf("host %s %s: HTTP %d", method, path, response.StatusCode)
	}
	return nil
}

// shimLoadManifest retries through a daemon restart, then falls back to the
// last manifest seen. The cache is availability state, not an optimization: a
// session born without chat_reply stays unable to answer for its entire life.
func shimLoadManifest(ctx context.Context, opts shimManifestLoadOptions) (Manifest, error) {
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = shimDefaultSleep
	}
	logf := opts.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	cachePath := shimCachePath(opts.CacheDir, opts.HostURL)

	var lastErr error
	for range shimManifestRetries {
		var manifest Manifest
		err := shimDoJSON(ctx, client, opts.HostURL, http.MethodGet, "/manifest", nil, &manifest)
		if err == nil && manifest.Name == "" {
			err = fmt.Errorf("manifest has no name")
		}
		if err == nil {
			if encoded, encodeErr := json.Marshal(manifest); encodeErr != nil {
				logf("manifest cache encode failed (non-fatal): %v", encodeErr)
			} else if mkdirErr := os.MkdirAll(opts.CacheDir, 0o700); mkdirErr != nil {
				logf("manifest cache write failed (non-fatal): %v", mkdirErr)
			} else if writeErr := os.WriteFile(cachePath, encoded, 0o600); writeErr != nil {
				logf("manifest cache write failed (non-fatal): %v", writeErr)
			}
			return manifest, nil
		}
		lastErr = err
		if err := sleep(ctx, shimManifestRetryWait); err != nil {
			lastErr = err
			break
		}
	}

	encoded, err := os.ReadFile(cachePath)
	if err == nil {
		var manifest Manifest
		if err := json.Unmarshal(encoded, &manifest); err == nil {
			logf("host unreachable — using cached manifest for %s", manifest.Name)
			return manifest, nil
		}
	}
	return Manifest{}, fmt.Errorf(
		"could not fetch /manifest from %s after %d attempts and no cache at %s: %v",
		opts.HostURL,
		shimManifestRetries,
		cachePath,
		lastErr,
	)
}

func shimCallTool(ctx context.Context, client *http.Client, hostURL, agent, name string, raw json.RawMessage) (*mcp.CallToolResult, error) {
	args := make(map[string]any)
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &args); err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "error: invalid tool arguments: " + err.Error()}},
				IsError: true,
			}, nil
		}
	}
	// This assignment is deliberately last. agent is the session's identity,
	// not a model argument; a model-supplied value can never override the env.
	args["agent"] = agent
	var response shimToolResponse
	err := shimDoJSON(ctx, client, hostURL, http.MethodPost, "/tool/"+name, args, &response)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "error: " + err.Error()}},
			IsError: true,
		}, nil
	}
	text := response.Text
	if text == "" {
		text = response.Error
	}
	if text == "" {
		text = "(no output)"
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
		IsError: response.IsError || response.Error != "",
	}, nil
}

func shimNewServer(manifest Manifest, opts MCPOptions, client *http.Client) *mcp.Server {
	version := manifest.Version
	if version == "" {
		version = "0.0.0"
	}
	server := mcp.NewServer(&mcp.Implementation{Name: manifest.Name, Version: version}, &mcp.ServerOptions{
		Instructions: manifest.Instructions,
	})
	for _, definition := range manifest.Tools {
		definition := definition
		inputSchema := any(definition.InputSchema)
		if definition.InputSchema == nil {
			inputSchema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		server.AddTool(&mcp.Tool{
			Name:        definition.Name,
			Description: definition.Description,
			InputSchema: inputSchema,
		}, func(ctx context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return shimCallTool(ctx, client, opts.HostURL, opts.Agent, definition.Name, request.Params.Arguments)
		})
	}
	return server
}

type shimEOFReadCloser struct {
	reader io.Reader
	closer io.Closer
	eof    chan struct{}
	once   sync.Once
}

func (r *shimEOFReadCloser) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if errors.Is(err, io.EOF) {
		r.once.Do(func() { close(r.eof) })
	}
	return n, err
}

func (r *shimEOFReadCloser) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

type shimNopWriteCloser struct{ io.Writer }

func (shimNopWriteCloser) Close() error { return nil }

// shimRunServer watches the same reader the SDK consumes, so EOF observation
// cannot steal protocol bytes. The watcher cancels the session and converts
// parent disappearance into a clean exit status.
func shimRunServer(ctx context.Context, server *mcp.Server, stdin io.Reader, stdout io.Writer) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	eof := make(chan struct{})
	reader := &shimEOFReadCloser{reader: stdin, eof: eof}
	go func() {
		select {
		case <-eof:
			cancel()
		case <-runCtx.Done():
		}
	}()

	err := server.Run(runCtx, &mcp.IOTransport{Reader: reader, Writer: shimNopWriteCloser{stdout}})
	select {
	case <-eof:
		return nil
	default:
	}
	return err
}

func shimWarnProtocol(manifest Manifest, logf shimLogFunc) {
	if manifest.Protocol != ManifestProtocol {
		// Loud, not fatal: the advertised tools may still be usable, and the
		// dangerous mismatch is an old protocol-1 shim aimed at this host.
		logf("WARNING: expected manifest protocol %d, got %d", ManifestProtocol, manifest.Protocol)
	}
}

func mcpMain(opts MCPOptions) error {
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[courier-mcp] "+format+"\n", args...)
	}
	ctx := context.Background()
	client := &http.Client{Timeout: 10 * time.Second}
	manifest, err := shimLoadManifest(ctx, shimManifestLoadOptions{
		HostURL:  opts.HostURL,
		CacheDir: opts.ManifestCacheDir,
		Client:   client,
		Log:      logf,
	})
	if err != nil {
		return err
	}
	logf("manifest loaded: %s protocol %d — %d tool(s)", manifest.Name, manifest.Protocol, len(manifest.Tools))
	shimWarnProtocol(manifest, logf)
	server := shimNewServer(manifest, opts, client)
	logf("connecting over stdio as %s — agent: %s host: %s", manifest.Name, opts.Agent, opts.HostURL)
	return shimRunServer(ctx, server, os.Stdin, os.Stdout)
}
