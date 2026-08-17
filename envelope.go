package main

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	EnvelopeSchema  = "courier/1"
	SchemaSkillPath = "/opt/skills/courier/SKILL.md"

	// These are delivery-surface ceilings, not formatting preferences. The
	// envelope is a pointer into the ledger because full payloads made the pane
	// unreadable and fed herdr's paste-chip stall class.
	MaxEnvelopeChars = 1500
	MaxEnvelopeLines = 12
	PreviewChars     = 100
	MaxUserChars     = 64
	MaxTriggerChars  = 32
)

var (
	attributeReplacer = strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
	)
	previewTagPattern = regexp.MustCompile(`<[^>]*>`)
)

// EnvelopeInput is deliberately independent of store rows. A dispatcher adapts
// its claimed delivery and event into this stable formatting boundary.
type EnvelopeInput struct {
	DeliveryID     string
	ConversationID string
	User           string
	Connector      string
	AttemptCount   int
	MetaJSON       string
	Content        string
	Read           bool
	PreviewOn      bool
}

// MsgFullInput carries the ledger facts needed by a read_message response.
type MsgFullInput struct {
	DeliveryID     string
	ConversationID string
	User           string
	Connector      string
	Status         string
	Content        string
	FirstRead      bool
	Settled        bool
}

// attr escapes a value going into an XML-ish attribute. This is a routing
// boundary, not cosmetic escaping: display names and upstream conversation ids
// must not be able to close an attribute and forge another delivery_id.
func attr(value string) string {
	return attributeReplacer.Replace(value)
}

// clipRunes clips by Unicode code points. The TypeScript predecessor counted
// UTF-16 code units; courier deliberately uses utf8.RuneCountInString so an
// astral-plane character counts once rather than twice.
func clipRunes(value string, limit int) string {
	if limit < 0 || utf8.RuneCountInString(value) <= limit {
		return value
	}
	if limit == 0 {
		return ""
	}

	runes := 0
	for byteIndex := range value {
		if runes == limit {
			return value[:byteIndex]
		}
		runes++
	}
	return value
}

// TriggerOf returns the connector-reported reason for delivery. Missing,
// malformed, empty, and non-string values are all omitted: the dispatcher
// cannot recover this fact and guessing would change whether an agent replies.
func TriggerOf(metaJSON string) (string, bool) {
	var meta map[string]any
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil || meta == nil {
		return "", false
	}
	value, ok := meta["trigger"].(string)
	if !ok {
		return "", false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	return clipRunes(value, MaxTriggerChars), true
}

// preview is one line and structurally incapable of carrying framing. Stripping
// every tag-shaped substring is the security property: content is intentionally
// unescaped when read in full, but a hostile </msg> must not become framing in
// the compact pointer shown in the agent's terminal.
func preview(content string) string {
	flat := previewTagPattern.ReplaceAllString(content, " ")
	flat = strings.Join(strings.Fields(flat), " ")
	if flat == "" {
		return "(no text — attachments or an empty body)"
	}
	if utf8.RuneCountInString(flat) <= PreviewChars {
		return flat
	}
	clipped := strings.TrimRightFunc(clipRunes(flat, PreviewChars), unicode.IsSpace)
	return clipped + "…"
}

// BuildEnvelope formats the courier/1 pointer. The ids, connector, and schema
// stay on the tag; the full body, context, and attachments stay in the ledger.
func BuildEnvelope(in EnvelopeInput) string {
	redelivery := in.AttemptCount - 1
	if redelivery < 0 {
		redelivery = 0
	}
	user := in.User
	if user == "" {
		user = "unknown"
	}
	user = clipRunes(user, MaxUserChars)
	trigger, hasTrigger := TriggerOf(in.MetaJSON)

	var tag strings.Builder
	tag.WriteString(`<msg delivery_id="`)
	tag.WriteString(attr(in.DeliveryID))
	tag.WriteString(`" conversation_id="`)
	tag.WriteString(attr(in.ConversationID))
	tag.WriteString(`" user="`)
	tag.WriteString(attr(user))
	tag.WriteString(`" connector="`)
	tag.WriteString(attr(in.Connector))
	tag.WriteString(`" redelivery="`)
	tag.WriteString(attr(strconv.Itoa(redelivery)))
	tag.WriteByte('"')
	if hasTrigger {
		tag.WriteString(` trigger="`)
		tag.WriteString(attr(trigger))
		tag.WriteByte('"')
	}
	tag.WriteString(` schema="`)
	tag.WriteString(attr(EnvelopeSchema))
	if in.PreviewOn || redelivery > 0 {
		tag.WriteString(`">`)
	} else {
		tag.WriteString(`"/>`)
	}

	var envelope strings.Builder
	envelope.Grow(tag.Len() + PreviewChars + 100)
	envelope.WriteString(tag.String())
	if in.PreviewOn {
		envelope.WriteByte('\n')
		envelope.WriteString(preview(in.Content))
	}
	if redelivery > 0 {
		envelope.WriteByte('\n')
		if !in.Read {
			envelope.WriteString("[redelivery ")
			envelope.WriteString(strconv.Itoa(redelivery))
			envelope.WriteString(", unread: already replied? mark_handled; otherwise read_message]")
		} else {
			envelope.WriteString("[redelivery ")
			envelope.WriteString(strconv.Itoa(redelivery))
			envelope.WriteString(", read/unsettled: do not reply twice; chat_reply or mark_handled]")
		}
	}
	if in.PreviewOn || redelivery > 0 {
		envelope.WriteString("\n</msg>")
	}

	return envelope.String()
}

const (
	msgFullJudgment = "[settle: chat_reply or mark_handled]"
	msgFullSettled  = "[already settled; history only]"
)

// BuildMsgFull formats read_message's successor response. The event content is
// deliberately verbatim, undecorated, and unescaped: this is the ledger payload
// the pointer withheld, and the agent must see exactly what the sender wrote.
func BuildMsgFull(in MsgFullInput) string {
	user := in.User
	if user == "" {
		user = "unknown"
	}
	read := "again"
	if in.FirstRead {
		read = "first"
	}

	var out strings.Builder
	out.WriteString(`<msg_full delivery_id="`)
	out.WriteString(attr(in.DeliveryID))
	out.WriteString(`" conversation_id="`)
	out.WriteString(attr(in.ConversationID))
	out.WriteString(`" user="`)
	out.WriteString(attr(user))
	out.WriteString(`" connector="`)
	out.WriteString(attr(in.Connector))
	out.WriteString(`" status="`)
	out.WriteString(attr(in.Status))
	out.WriteString(`" read="`)
	out.WriteString(attr(read))
	out.WriteString(`" schema="`)
	out.WriteString(attr(EnvelopeSchema))
	out.WriteString("\">\n")
	out.WriteString(in.Content)
	out.WriteByte('\n')
	if in.Settled {
		out.WriteString(msgFullSettled)
	} else {
		out.WriteString(msgFullJudgment)
	}
	out.WriteString("\n</msg_full>")
	return out.String()
}
