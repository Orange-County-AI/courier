package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const GmailMaxBodyChars = 20_000

var (
	gmailAddressPattern = regexp.MustCompile(`^(.*?)<([^<>]+)>\s*$`)
	gmailDiscardHTML    = []*regexp.Regexp{
		regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script>`),
		regexp.MustCompile(`(?is)<style\b[^>]*>.*?</style>`),
		regexp.MustCompile(`(?is)<head\b[^>]*>.*?</head>`),
	}
	gmailHTMLComment = regexp.MustCompile(`(?s)<!--.*?-->`)
	gmailBreak       = regexp.MustCompile(`(?i)<br\s*/?>`)
	gmailListItem    = regexp.MustCompile(`(?i)<li\b[^>]*>`)
	gmailBlockClose  = regexp.MustCompile(`(?i)</(?:p|div|tr|h[1-6]|blockquote|table|ul|ol)>`)
	gmailLink        = regexp.MustCompile(`(?is)<a\b[^>]*href="([^"]*)"[^>]*>(.*?)</a>`)
	gmailTag         = regexp.MustCompile(`<[^>]+>`)
	gmailLineSpace   = regexp.MustCompile(`[ \t]+\n`)
	gmailManyLines   = regexp.MustCompile(`\n{3,}`)
	gmailReplyPrefix = regexp.MustCompile(`(?i)^\s*re\s*:`)
)

type GmailHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type GmailPartBody struct {
	AttachmentID string `json:"attachmentId,omitempty"`
	Size         int64  `json:"size,omitempty"`
	Data         string `json:"data,omitempty"`
}

type GmailPart struct {
	PartID   string         `json:"partId,omitempty"`
	MIMEType string         `json:"mimeType,omitempty"`
	Filename string         `json:"filename,omitempty"`
	Headers  []GmailHeader  `json:"headers,omitempty"`
	Body     *GmailPartBody `json:"body,omitempty"`
	Parts    []GmailPart    `json:"parts,omitempty"`
}

type GmailMessage struct {
	ID           string     `json:"id"`
	ThreadID     string     `json:"threadId"`
	LabelIDs     []string   `json:"labelIds,omitempty"`
	Snippet      string     `json:"snippet,omitempty"`
	HistoryID    string     `json:"historyId,omitempty"`
	InternalDate string     `json:"internalDate,omitempty"`
	Payload      *GmailPart `json:"payload,omitempty"`
}

type GmailAddress struct {
	Name  string
	Email string
}

type GmailAttachmentRef struct {
	Filename     string
	MIME         string
	Size         int64
	AttachmentID string
}

type GmailSavedAttachment struct {
	Name    string
	Path    string
	MIME    string
	IsImage bool
	Note    string
}

type GmailNormalized struct {
	GmailID     string
	ThreadID    string
	MessageID   string
	Subject     string
	From        GmailAddress
	To          string
	CC          string
	Date        string
	Body        string
	Content     string
	Meta        map[string]string
	Attachments []GmailAttachmentRef
}

type GmailAccountConfig struct {
	Email         string
	TokenCommand  string
	LabelsRequire []string
	LabelsExclude []string
	PollSeconds   float64
}

type gmailAccountWire struct {
	Email         any `json:"email"`
	TokenCommand  any `json:"token_command"`
	LabelsRequire any `json:"labels_require"`
	LabelsExclude any `json:"labels_exclude"`
	PollSeconds   any `json:"poll_seconds"`
}

func ParseGmailAccounts(input string) ([]GmailAccountConfig, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal([]byte(input), &raw); err != nil {
		return nil, fmt.Errorf("accounts config is not valid JSON: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("accounts config must be a non-empty JSON array")
	}
	accounts := make([]GmailAccountConfig, 0, len(raw))
	for index, item := range raw {
		var wire gmailAccountWire
		if err := json.Unmarshal(item, &wire); err != nil {
			return nil, fmt.Errorf("accounts[%d]: %w", index, err)
		}
		email, ok := wire.Email.(string)
		if !ok || !strings.Contains(email, "@") {
			return nil, fmt.Errorf("accounts[%d]: \"email\" must be an email address", index)
		}
		command, ok := wire.TokenCommand.(string)
		if !ok || strings.TrimSpace(command) == "" {
			return nil, fmt.Errorf("accounts[%d] (%s): \"token_command\" is required", index, email)
		}
		labelsRequire, err := gmailStringList(wire.LabelsRequire)
		if err != nil {
			return nil, fmt.Errorf("accounts[%d] (%s): \"labels_require\" must be an array of label names", index, email)
		}
		labelsExclude, err := gmailStringList(wire.LabelsExclude)
		if err != nil {
			return nil, fmt.Errorf("accounts[%d] (%s): \"labels_exclude\" must be an array of label names", index, email)
		}
		pollSeconds := 0.0
		if wire.PollSeconds != nil {
			value, ok := wire.PollSeconds.(float64)
			if !ok || value <= 0 {
				return nil, fmt.Errorf("accounts[%d] (%s): \"poll_seconds\" must be a positive number", index, email)
			}
			pollSeconds = value
		}
		accounts = append(accounts, GmailAccountConfig{
			Email: strings.ToLower(strings.TrimSpace(email)), TokenCommand: strings.TrimSpace(command),
			LabelsRequire: labelsRequire, LabelsExclude: labelsExclude, PollSeconds: pollSeconds,
		})
	}
	return accounts, nil
}

func gmailStringList(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("not an array")
	}
	result := make([]string, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("item %d is not a string", index)
		}
		result[index] = text
	}
	return result, nil
}

func GmailHeaderValue(headers []GmailHeader, name string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Name, name) {
			return header.Value
		}
	}
	return ""
}

func ParseGmailAddress(raw string) GmailAddress {
	raw = strings.TrimSpace(raw)
	if match := gmailAddressPattern.FindStringSubmatch(raw); match != nil {
		name := strings.TrimSpace(match[1])
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = strings.TrimSpace(name[1 : len(name)-1])
		}
		return GmailAddress{Name: name, Email: strings.ToLower(strings.TrimSpace(match[2]))}
	}
	bare := strings.Trim(raw, "<>")
	if strings.Contains(bare, "@") {
		return GmailAddress{Email: strings.ToLower(bare)}
	}
	return GmailAddress{}
}

func ParseGmailAddressList(value string) []GmailAddress {
	if value == "" {
		return nil
	}
	parts := make([]string, 0, 2)
	quoted := false
	start := 0
	for index, char := range value {
		if char == '"' {
			quoted = !quoted
		} else if char == ',' && !quoted {
			parts = append(parts, value[start:index])
			start = index + 1
		}
	}
	parts = append(parts, value[start:])
	addresses := make([]GmailAddress, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			addresses = append(addresses, ParseGmailAddress(part))
		}
	}
	return addresses
}

func DecodeGmailBase64URL(data string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(data, "="))
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func GmailHTMLToText(source string) string {
	text := source
	for _, pattern := range gmailDiscardHTML {
		text = pattern.ReplaceAllString(text, "")
	}
	text = gmailHTMLComment.ReplaceAllString(text, "")
	text = gmailBreak.ReplaceAllString(text, "\n")
	text = gmailListItem.ReplaceAllString(text, "\n- ")
	text = gmailBlockClose.ReplaceAllString(text, "\n")
	text = gmailLink.ReplaceAllStringFunc(text, func(link string) string {
		match := gmailLink.FindStringSubmatch(link)
		target := match[1]
		label := strings.TrimSpace(gmailTag.ReplaceAllString(match[2], ""))
		if label != "" && label != target {
			return label + " (" + target + ")"
		}
		return target
	})
	text = gmailTag.ReplaceAllString(text, "")
	text = html.UnescapeString(text)
	text = gmailLineSpace.ReplaceAllString(text, "\n")
	text = gmailManyLines.ReplaceAllString(text, "\n\n")
	return strings.TrimSpace(text)
}

func ExtractGmailBody(payload *GmailPart) string {
	if payload == nil {
		return ""
	}
	plain := make([]string, 0, 1)
	htmlBodies := make([]string, 0, 1)
	var walk func(GmailPart)
	walk = func(part GmailPart) {
		if len(part.Parts) != 0 {
			for _, child := range part.Parts {
				walk(child)
			}
			return
		}
		if part.Filename != "" || part.Body == nil || part.Body.Data == "" {
			return
		}
		decoded, err := DecodeGmailBase64URL(part.Body.Data)
		if err != nil {
			return
		}
		mime := strings.ToLower(part.MIMEType)
		if strings.HasPrefix(mime, "text/plain") {
			plain = append(plain, decoded)
		} else if strings.HasPrefix(mime, "text/html") {
			htmlBodies = append(htmlBodies, decoded)
		}
	}
	walk(*payload)
	if len(plain) != 0 {
		return strings.TrimSpace(strings.Join(plain, "\n"))
	}
	if len(htmlBodies) != 0 {
		return GmailHTMLToText(strings.Join(htmlBodies, "\n"))
	}
	return ""
}

func ExtractGmailAttachments(payload *GmailPart) []GmailAttachmentRef {
	if payload == nil {
		return nil
	}
	var result []GmailAttachmentRef
	var walk func(GmailPart)
	walk = func(part GmailPart) {
		if part.Filename != "" && part.Body != nil && part.Body.AttachmentID != "" {
			mime := part.MIMEType
			if mime == "" {
				mime = "application/octet-stream"
			}
			result = append(result, GmailAttachmentRef{
				Filename: part.Filename, MIME: mime, Size: part.Body.Size, AttachmentID: part.Body.AttachmentID,
			})
		}
		for _, child := range part.Parts {
			walk(child)
		}
	}
	walk(*payload)
	return result
}

var (
	DefaultGmailRequireLabels = []string{"INBOX"}
	DefaultGmailExcludeLabels = []string{
		"SPAM", "TRASH", "DRAFT", "SENT", "CATEGORY_PROMOTIONS",
		"CATEGORY_SOCIAL", "CATEGORY_UPDATES", "CATEGORY_FORUMS",
	}
)

type GmailFilterOptions struct {
	LabelsRequire []string
	LabelsExclude []string
}

type GmailDeliveryVerdict struct {
	Deliver bool
	Reason  string
}

func GmailDeliveryDecision(message GmailMessage, fromEmail, accountEmail string, options GmailFilterOptions) GmailDeliveryVerdict {
	if fromEmail != "" && strings.EqualFold(fromEmail, accountEmail) {
		return GmailDeliveryVerdict{Reason: "self-authored (loop prevention)"}
	}
	labels := make(map[string]struct{}, len(message.LabelIDs))
	for _, label := range message.LabelIDs {
		labels[label] = struct{}{}
	}
	required := options.LabelsRequire
	if required == nil {
		required = DefaultGmailRequireLabels
	}
	for _, label := range required {
		if _, found := labels[label]; !found {
			return GmailDeliveryVerdict{Reason: "missing required label " + label}
		}
	}
	excluded := options.LabelsExclude
	if excluded == nil {
		excluded = DefaultGmailExcludeLabels
	}
	for _, label := range excluded {
		if _, found := labels[label]; found {
			return GmailDeliveryVerdict{Reason: "excluded label " + label}
		}
	}
	return GmailDeliveryVerdict{Deliver: true, Reason: "ok"}
}

func NormalizeGmailMessage(accountEmail string, message GmailMessage, options GmailFilterOptions) *GmailNormalized {
	var headers []GmailHeader
	if message.Payload != nil {
		headers = message.Payload.Headers
	}
	from := ParseGmailAddress(GmailHeaderValue(headers, "From"))
	if !GmailDeliveryDecision(message, from.Email, accountEmail, options).Deliver {
		return nil
	}
	to := GmailHeaderValue(headers, "To")
	cc := GmailHeaderValue(headers, "Cc")
	subject := GmailHeaderValue(headers, "Subject")
	if subject == "" {
		subject = "(no subject)"
	}
	messageID := GmailHeaderValue(headers, "Message-ID")
	date := GmailHeaderValue(headers, "Date")
	if date == "" && message.InternalDate != "" {
		if milliseconds, err := strconv.ParseInt(message.InternalDate, 10, 64); err == nil {
			date = time.UnixMilli(milliseconds).UTC().Format("2006-01-02T15:04:05.000Z")
		}
	}
	body := ExtractGmailBody(message.Payload)
	if utf8.RuneCountInString(body) > GmailMaxBodyChars {
		runes := []rune(body)
		body = string(runes[:GmailMaxBodyChars]) + fmt.Sprintf("\n\n[... body truncated at %d chars]", GmailMaxBodyChars)
	}
	attachments := ExtractGmailAttachments(message.Payload)
	if body == "" {
		if strings.TrimSpace(message.Snippet) != "" {
			body = strings.TrimSpace(message.Snippet)
		} else if len(attachments) != 0 {
			suffix := ""
			if len(attachments) > 1 {
				suffix = "s"
			}
			body = fmt.Sprintf("(no text body — %d attachment%s)", len(attachments), suffix)
		} else {
			body = "(empty message)"
		}
	}
	fromLabel := "an unknown sender"
	if from.Name != "" {
		email := from.Email
		if email == "" {
			email = "unknown"
		}
		fromLabel = from.Name + " <" + email + ">"
	} else if from.Email != "" {
		fromLabel = from.Email
	}
	lines := []string{"New email to " + accountEmail + " from " + fromLabel + ":", "", "From: " + gmailValueOr(GmailHeaderValue(headers, "From"), "(unknown)")}
	if to != "" {
		lines = append(lines, "To: "+to)
	}
	if cc != "" {
		lines = append(lines, "Cc: "+cc)
	}
	lines = append(lines, "Subject: "+subject)
	if date != "" {
		lines = append(lines, "Date: "+date)
	}
	lines = append(lines, "", body)
	meta := map[string]string{
		"source": "gmail-channel", "type": "email", "account": accountEmail,
		"gmail_id": message.ID, "thread_id": message.ThreadID, "subject": subject,
	}
	gmailPutMeta(meta, "from_email", from.Email)
	gmailPutMeta(meta, "from_name", from.Name)
	gmailPutMeta(meta, "message_id", messageID)
	gmailPutMeta(meta, "date", date)
	if len(attachments) != 0 {
		meta["attachments"] = strconv.Itoa(len(attachments))
	}
	return &GmailNormalized{
		GmailID: message.ID, ThreadID: message.ThreadID, MessageID: messageID, Subject: subject,
		From: from, To: to, CC: cc, Date: date, Body: body, Content: strings.Join(lines, "\n"),
		Meta: meta, Attachments: attachments,
	}
}

func gmailPutMeta(meta map[string]string, key, value string) {
	if value != "" {
		meta[key] = value
	}
}

func FormatGmailAttachmentLine(attachment GmailSavedAttachment) string {
	if attachment.Path == "" {
		note := attachment.Note
		if note == "" {
			note = "unavailable"
		}
		return fmt.Sprintf("- %s (%s) — %s", attachment.Name, attachment.MIME, note)
	}
	hint := "Read this path to open it"
	if attachment.IsImage {
		hint = "image — Read this path to view it"
	}
	return fmt.Sprintf("- %s (%s): %s  [%s]", attachment.Name, attachment.MIME, attachment.Path, hint)
}

func FormatGmailAttachmentBlock(saved []GmailSavedAttachment) string {
	lines := make([]string, len(saved))
	for index, attachment := range saved {
		lines[index] = FormatGmailAttachmentLine(attachment)
	}
	return fmt.Sprintf("--- attachments (%d) ---\n%s", len(saved), strings.Join(lines, "\n"))
}

type GmailMIMEOptions struct {
	From       string
	To         string
	CC         string
	Subject    string
	Body       string
	InReplyTo  string
	References string
}

func EncodeGmailHeaderText(value string) string {
	for _, char := range value {
		if char < 0x20 || char > 0x7e {
			return "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(value)) + "?="
		}
	}
	return value
}

func BuildGmailMIME(options GmailMIMEOptions) string {
	headers := []string{"From: " + options.From, "To: " + options.To}
	if options.CC != "" {
		headers = append(headers, "Cc: "+options.CC)
	}
	headers = append(headers, "Subject: "+EncodeGmailHeaderText(options.Subject))
	if options.InReplyTo != "" {
		headers = append(headers, "In-Reply-To: "+options.InReplyTo)
	}
	if options.References != "" {
		headers = append(headers, "References: "+options.References)
	}
	headers = append(headers, "MIME-Version: 1.0", "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: base64")
	encoded := base64.StdEncoding.EncodeToString([]byte(options.Body))
	var wrapped strings.Builder
	wrapped.Grow(len(encoded) + len(encoded)/76*2)
	for len(encoded) >= 76 {
		wrapped.WriteString(encoded[:76])
		wrapped.WriteString("\r\n")
		encoded = encoded[76:]
	}
	wrapped.WriteString(encoded)
	return strings.Join(headers, "\r\n") + "\r\n\r\n" + wrapped.String()
}

func GmailBase64URL(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

type GmailReplyHeaderValues struct {
	InReplyTo  string
	References string
}

func GmailReplyHeaders(messageID, references string) GmailReplyHeaderValues {
	result := GmailReplyHeaderValues{InReplyTo: messageID}
	result.References = strings.TrimSpace(strings.Join(gmailNonEmptyStrings(references, messageID), " "))
	return result
}

func gmailNonEmptyStrings(values ...string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

func GmailReplySubject(subject string) string {
	if gmailReplyPrefix.MatchString(subject) {
		return subject
	}
	return "Re: " + subject
}
