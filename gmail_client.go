package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	GmailTokenMaxAge         = 55 * time.Minute
	gmailTokenCommandTimeout = 60 * time.Second
	gmailTokenOutputLimit    = 4 * 1024 * 1024
)

type GmailHTTPError struct {
	Status  int
	Message string
}

func (e *GmailHTTPError) Error() string { return e.Message }

type gmailTokenCall struct {
	done  chan struct{}
	token string
	err   error
}

type GmailTokenProviderConfig struct {
	Command string
	Label   string
	MaxAge  time.Duration
	Timeout time.Duration
	Now     func() time.Time
	Run     func(context.Context, string) (stdout, stderr string, exitCode int, err error)
}

type GmailTokenProvider struct {
	command string
	label   string
	maxAge  time.Duration
	timeout time.Duration
	now     func() time.Time
	run     func(context.Context, string) (stdout, stderr string, exitCode int, err error)

	mu        sync.Mutex
	token     string
	fetchedAt time.Time
	inflight  *gmailTokenCall
}

func NewGmailTokenProvider(command, label string) *GmailTokenProvider {
	return NewGmailTokenProviderWithConfig(GmailTokenProviderConfig{Command: command, Label: label})
}

func NewGmailTokenProviderWithConfig(config GmailTokenProviderConfig) *GmailTokenProvider {
	maxAge := config.MaxAge
	if maxAge == 0 {
		maxAge = GmailTokenMaxAge
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = gmailTokenCommandTimeout
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	run := config.Run
	if run == nil {
		run = gmailRunTokenCommand
	}
	return &GmailTokenProvider{
		command: config.Command, label: config.Label, maxAge: maxAge,
		timeout: timeout, now: now, run: run,
	}
}

func (p *GmailTokenProvider) Get(ctx context.Context) (string, error) {
	p.mu.Lock()
	if p.token != "" && p.now().Sub(p.fetchedAt) < p.maxAge {
		token := p.token
		p.mu.Unlock()
		return token, nil
	}
	if call := p.inflight; call != nil {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-call.done:
			return call.token, call.err
		}
	}
	call := &gmailTokenCall{done: make(chan struct{})}
	p.inflight = call
	p.mu.Unlock()

	call.token, call.err = p.refresh()
	p.mu.Lock()
	if call.err == nil {
		p.token = call.token
		p.fetchedAt = p.now()
	}
	p.inflight = nil
	close(call.done)
	p.mu.Unlock()
	return call.token, call.err
}

func (p *GmailTokenProvider) Invalidate() {
	p.mu.Lock()
	p.token = ""
	p.fetchedAt = time.Time{}
	p.mu.Unlock()
}

func (p *GmailTokenProvider) refresh() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	stdout, stderr, exitCode, err := p.run(ctx, p.command)
	if err != nil {
		stderr = strings.TrimSpace(stderr)
		// Keep the head, never the tail: token commands are credential-producing
		// subprocesses, so a token or client secret is far likelier at the end.
		if len(stderr) > 500 {
			stderr = stderr[:500]
		}
		if exitCode < 0 {
			return "", fmt.Errorf("token_command for %s failed (error): %s", p.label, stderr)
		}
		return "", fmt.Errorf("token_command for %s failed (%d): %s", p.label, exitCode, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	token := strings.TrimSpace(lines[len(lines)-1])
	if token == "" {
		return "", fmt.Errorf("token_command for %s printed no token", p.label)
	}
	return token, nil
}

type gmailLimitedCapture struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (c *gmailLimitedCapture) Write(data []byte) (int, error) {
	remaining := c.limit - c.buffer.Len()
	if remaining > 0 {
		written := min(remaining, len(data))
		_, _ = c.buffer.Write(data[:written])
	}
	if len(data) > remaining {
		c.overflow = true
	}
	return len(data), nil
}

// PROVENANCE: the token_command contract is ported from the TypeScript
// predecessor. This one exec intentionally uses bash -c: the command is trusted
// operator configuration and is expected to be a pipeline. Chat input never
// reaches a shell anywhere in courier.
func gmailRunTokenCommand(ctx context.Context, command string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	stdout := &gmailLimitedCapture{limit: gmailTokenOutputLimit}
	stderr := &gmailLimitedCapture{limit: gmailTokenOutputLimit}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	if stdout.overflow || stderr.overflow {
		return stdout.buffer.String(), stderr.buffer.String(), exitCode, fmt.Errorf("token_command output exceeded %d bytes", gmailTokenOutputLimit)
	}
	return stdout.buffer.String(), stderr.buffer.String(), exitCode, err
}

type gmailTokenSource interface {
	Get(context.Context) (string, error)
	Invalidate()
}

type GmailProfile struct {
	EmailAddress  string `json:"emailAddress"`
	MessagesTotal int64  `json:"messagesTotal"`
	ThreadsTotal  int64  `json:"threadsTotal"`
	HistoryID     string `json:"historyId"`
}

type GmailHistoryMessage struct {
	ID       string   `json:"id"`
	ThreadID string   `json:"threadId"`
	LabelIDs []string `json:"labelIds,omitempty"`
}

type GmailHistoryAdded struct {
	Message GmailHistoryMessage `json:"message"`
}

type GmailHistoryRecord struct {
	ID            string              `json:"id"`
	MessagesAdded []GmailHistoryAdded `json:"messagesAdded,omitempty"`
}

type GmailHistoryPage struct {
	History       []GmailHistoryRecord `json:"history,omitempty"`
	HistoryID     string               `json:"historyId"`
	NextPageToken string               `json:"nextPageToken,omitempty"`
}

type GmailThreadMetadata struct {
	ID       string         `json:"id"`
	Messages []GmailMessage `json:"messages"`
}

type GmailSentMessage struct {
	ID       string   `json:"id"`
	ThreadID string   `json:"threadId"`
	LabelIDs []string `json:"labelIds,omitempty"`
}

type GmailClientConfig struct {
	Email      string
	Tokens     gmailTokenSource
	BaseURL    string
	HTTPClient *http.Client
}

type GmailClient struct {
	email   string
	tokens  gmailTokenSource
	baseURL string
	client  *http.Client
}

func NewGmailClient(config GmailClientConfig) (*GmailClient, error) {
	if strings.TrimSpace(config.Email) == "" || config.Tokens == nil {
		return nil, fmt.Errorf("gmail client requires email and token provider")
	}
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://gmail.googleapis.com/gmail/v1/users/" + url.PathEscape(config.Email)
	}
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &GmailClient{email: config.Email, tokens: config.Tokens, baseURL: baseURL, client: client}, nil
}

func (c *GmailClient) request(ctx context.Context, method, path string, body any, output any) error {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	for attempt := range 2 {
		token, err := c.tokens.Get(ctx)
		if err != nil {
			return err
		}
		var requestBody io.Reader
		if encoded != nil {
			requestBody = bytes.NewReader(encoded)
		}
		request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, requestBody)
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if encoded != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := c.client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode == http.StatusUnauthorized && attempt == 0 {
			_ = response.Body.Close()
			c.tokens.Invalidate()
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 300))
			_ = response.Body.Close()
			return &GmailHTTPError{Status: response.StatusCode, Message: fmt.Sprintf("gmail %s %s: HTTP %d %s", method, path, response.StatusCode, string(responseBody))}
		}
		if output == nil {
			_ = response.Body.Close()
			return nil
		}
		decodeErr := json.NewDecoder(response.Body).Decode(output)
		closeErr := response.Body.Close()
		return errors.Join(decodeErr, closeErr)
	}
	return fmt.Errorf("gmail request exhausted authentication retry")
}

func (c *GmailClient) GetProfile(ctx context.Context) (GmailProfile, error) {
	var profile GmailProfile
	err := c.request(ctx, http.MethodGet, "/profile", nil, &profile)
	return profile, err
}

func (c *GmailClient) HistoryList(ctx context.Context, startHistoryID, pageToken string) (GmailHistoryPage, error) {
	params := url.Values{
		"startHistoryId": {startHistoryID}, "historyTypes": {"messageAdded"},
		"labelId": {"INBOX"}, "maxResults": {"500"},
	}
	if pageToken != "" {
		params.Set("pageToken", pageToken)
	}
	var page GmailHistoryPage
	err := c.request(ctx, http.MethodGet, "/history?"+params.Encode(), nil, &page)
	return page, err
}

func (c *GmailClient) GetMessage(ctx context.Context, id, format string) (GmailMessage, error) {
	if format == "" {
		format = "full"
	}
	var message GmailMessage
	err := c.request(ctx, http.MethodGet, "/messages/"+url.PathEscape(id)+"?format="+url.QueryEscape(format), nil, &message)
	return message, err
}

func (c *GmailClient) GetAttachment(ctx context.Context, messageID, attachmentID string) (size int64, data string, err error) {
	var attachment struct {
		Size int64  `json:"size"`
		Data string `json:"data"`
	}
	err = c.request(ctx, http.MethodGet, "/messages/"+url.PathEscape(messageID)+"/attachments/"+url.PathEscape(attachmentID), nil, &attachment)
	return attachment.Size, attachment.Data, err
}

func (c *GmailClient) GetThreadMetadata(ctx context.Context, threadID string) (GmailThreadMetadata, error) {
	params := url.Values{"format": {"metadata"}}
	for _, header := range []string{"Message-ID", "References", "Subject", "From"} {
		params.Add("metadataHeaders", header)
	}
	var thread GmailThreadMetadata
	err := c.request(ctx, http.MethodGet, "/threads/"+url.PathEscape(threadID)+"?"+params.Encode(), nil, &thread)
	return thread, err
}

func (c *GmailClient) Send(ctx context.Context, raw, threadID string) (GmailSentMessage, error) {
	body := map[string]string{"raw": raw}
	if threadID != "" {
		body["threadId"] = threadID
	}
	var sent GmailSentMessage
	err := c.request(ctx, http.MethodPost, "/messages/send", body, &sent)
	return sent, err
}
