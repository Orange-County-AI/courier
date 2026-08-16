package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	telegramDefaultAPIBase     = "https://api.telegram.org"
	telegramMaxResponseBytes   = 1 << 20
	telegramMaxAttachmentBytes = 50 * 1024 * 1024
)

type telegramAPI interface {
	SendMessage(context.Context, telegramSendMessage) (int64, error)
	SendChatAction(context.Context, string, int64, string) error
	SetReaction(context.Context, string, int64, string) error
	EditMessageText(context.Context, string, int64, string) error
	DownloadFile(context.Context, string) (string, error)
}

type TelegramClientConfig struct {
	Token         string
	BaseURL       string
	AttachmentDir string
	HTTPClient    *http.Client
}

type TelegramClient struct {
	token         string
	baseURL       string
	attachmentDir string
	httpClient    *http.Client
}

type TelegramAPIError struct {
	Method      string
	Status      int
	ErrorCode   int
	Description string
}

func (e *TelegramAPIError) Error() string {
	if e.ErrorCode != 0 {
		return fmt.Sprintf("telegram %s: API %d: %s", e.Method, e.ErrorCode, e.Description)
	}
	return fmt.Sprintf("telegram %s: HTTP %d: %s", e.Method, e.Status, e.Description)
}

type telegramSendMessage struct {
	ChatID           string
	MessageThreadID  int64
	ReplyToMessageID int64
	Text             string
}

type telegramResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
}

type telegramSentMessage struct {
	MessageID int64 `json:"message_id"`
}

type telegramFile struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FilePath     string `json:"file_path"`
}

func NewTelegramClient(config TelegramClientConfig) (*TelegramClient, error) {
	token := strings.TrimSpace(config.Token)
	if token == "" {
		return nil, errors.New("telegram client requires a bot token")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if baseURL == "" {
		baseURL = telegramDefaultAPIBase
	}
	attachmentDir := strings.TrimSpace(config.AttachmentDir)
	if attachmentDir == "" {
		attachmentDir = "./data/attachments/telegram"
	}
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &TelegramClient{
		token: token, baseURL: baseURL, attachmentDir: attachmentDir, httpClient: client,
	}, nil
}

func (c *TelegramClient) SendMessage(ctx context.Context, message telegramSendMessage) (int64, error) {
	body := map[string]any{"chat_id": message.ChatID, "text": message.Text}
	if message.MessageThreadID != 0 {
		body["message_thread_id"] = message.MessageThreadID
	}
	if message.ReplyToMessageID != 0 {
		body["reply_parameters"] = map[string]int64{"message_id": message.ReplyToMessageID}
	}
	var sent telegramSentMessage
	if err := c.call(ctx, "sendMessage", body, &sent); err != nil {
		return 0, err
	}
	if sent.MessageID == 0 {
		return 0, errors.New("telegram sendMessage: API confirmed success without a message_id")
	}
	return sent.MessageID, nil
}

func (c *TelegramClient) SendChatAction(ctx context.Context, chatID string, threadID int64, action string) error {
	body := map[string]any{"chat_id": chatID, "action": action}
	if threadID != 0 {
		body["message_thread_id"] = threadID
	}
	return c.call(ctx, "sendChatAction", body, nil)
}

func (c *TelegramClient) SetReaction(ctx context.Context, chatID string, messageID int64, emoji string) error {
	reaction := []map[string]string(nil)
	if emoji != "" {
		reaction = []map[string]string{{"type": "emoji", "emoji": emoji}}
	}
	return c.call(ctx, "setMessageReaction", map[string]any{
		"chat_id": chatID, "message_id": messageID, "reaction": reaction,
	}, nil)
}

func (c *TelegramClient) EditMessageText(ctx context.Context, chatID string, messageID int64, text string) error {
	return c.call(ctx, "editMessageText", map[string]any{
		"chat_id": chatID, "message_id": messageID, "text": text,
	}, nil)
}

func (c *TelegramClient) DownloadFile(ctx context.Context, fileID string) (string, error) {
	var file telegramFile
	if err := c.call(ctx, "getFile", map[string]string{"file_id": fileID}, &file); err != nil {
		return "", err
	}
	if file.FilePath == "" || file.FileUniqueID == "" {
		return "", errors.New("telegram getFile: API confirmed success without a usable file path")
	}
	if err := os.MkdirAll(c.attachmentDir, 0o755); err != nil {
		return "", fmt.Errorf("create Telegram attachment directory: %w", err)
	}
	extension := filepath.Ext(file.FilePath)
	path := filepath.Join(c.attachmentDir, telegramSafeFilename(file.FileUniqueID)+extension)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat Telegram attachment: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.fileEndpoint(file.FilePath), nil)
	if err != nil {
		return "", errors.New("telegram download: invalid request")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		// net/http errors usually embed request.URL. That URL contains the bot
		// token, so the transport detail must not escape this boundary.
		return "", errors.New("telegram download: request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("telegram download: HTTP %d", response.StatusCode)
	}
	contents, err := io.ReadAll(io.LimitReader(response.Body, telegramMaxAttachmentBytes+1))
	if err != nil {
		return "", errors.New("telegram download: response read failed")
	}
	if len(contents) > telegramMaxAttachmentBytes {
		return "", fmt.Errorf("telegram download: attachment exceeds %d bytes", telegramMaxAttachmentBytes)
	}
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		return "", fmt.Errorf("write Telegram attachment: %w", err)
	}
	return path, nil
}

func (c *TelegramClient) call(ctx context.Context, method string, body any, result any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("telegram %s: encode request: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(method), bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("telegram %s: invalid request", method)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		// Never wrap the net/http error: url.Error includes the token-bearing URL.
		return fmt.Errorf("telegram %s: request failed", method)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, telegramMaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("telegram %s: response read failed", method)
	}
	if len(payload) > telegramMaxResponseBytes {
		return fmt.Errorf("telegram %s: response too large", method)
	}
	var envelope telegramResponse[json.RawMessage]
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("telegram %s: invalid response (HTTP %d)", method, response.StatusCode)
	}
	if !envelope.OK || response.StatusCode < 200 || response.StatusCode >= 300 {
		description := c.redact(envelope.Description)
		if description == "" {
			description = http.StatusText(response.StatusCode)
		}
		return &TelegramAPIError{
			Method: method, Status: response.StatusCode, ErrorCode: envelope.ErrorCode, Description: description,
		}
	}
	if result != nil {
		if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
			return fmt.Errorf("telegram %s: API confirmed success without a result", method)
		}
		if err := json.Unmarshal(envelope.Result, result); err != nil {
			return fmt.Errorf("telegram %s: invalid result", method)
		}
	}
	return nil
}

func (c *TelegramClient) endpoint(method string) string {
	return c.baseURL + "/bot" + c.token + "/" + method
}

func (c *TelegramClient) fileEndpoint(filePath string) string {
	return c.baseURL + "/file/bot" + c.token + "/" + strings.TrimLeft(filePath, "/")
}

func (c *TelegramClient) redact(value string) string {
	if value == "" {
		return ""
	}
	return strings.ReplaceAll(value, c.token, "[REDACTED]")
}

func telegramSafeFilename(value string) string {
	value = strings.Map(func(char rune) rune {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			return char
		}
		return '_'
	}, value)
	if len(value) > 120 {
		value = value[len(value)-120:]
	}
	if value == "" {
		return "file"
	}
	return value
}
