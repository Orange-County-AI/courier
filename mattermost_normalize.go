package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	MattermostName = "mattermost"
	MMWatermarkKey = "mattermost:watermark"
)

type MMFileMeta struct {
	ID        string `json:"id"`
	Name      string `json:"name,omitempty"`
	Extension string `json:"extension,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
	Size      int64  `json:"size,omitempty"`
}

type MMPostMetadata struct {
	Files []MMFileMeta `json:"files,omitempty"`
}

type MMPost struct {
	ID        string         `json:"id"`
	RootID    string         `json:"root_id"`
	ChannelID string         `json:"channel_id"`
	UserID    string         `json:"user_id"`
	Message   string         `json:"message"`
	CreateAt  int64          `json:"create_at"`
	EditAt    int64          `json:"edit_at,omitempty"`
	DeleteAt  int64          `json:"delete_at,omitempty"`
	UpdateAt  int64          `json:"update_at,omitempty"`
	Props     map[string]any `json:"props,omitempty"`
	FileIDs   []string       `json:"file_ids,omitempty"`
	Metadata  MMPostMetadata `json:"metadata,omitempty"`
}

type MMPostedData struct {
	ChannelType        string `json:"channel_type,omitempty"`
	ChannelName        string `json:"channel_name,omitempty"`
	ChannelDisplayName string `json:"channel_display_name,omitempty"`
	SenderName         string `json:"sender_name,omitempty"`
	TeamID             string `json:"team_id,omitempty"`
	Post               string `json:"post,omitempty"`
	Mentions           string `json:"mentions,omitempty"`
	DeleteBy           string `json:"delete_by,omitempty"`
}

type MMEvent struct {
	Event     string        `json:"event,omitempty"`
	Data      *MMPostedData `json:"data,omitempty"`
	Broadcast any           `json:"broadcast,omitempty"`
	Seq       int64         `json:"seq,omitempty"`
}

type MMTrigger string

const (
	MMTriggerDM      MMTrigger = "dm"
	MMTriggerMention MMTrigger = "mention"
	MMTriggerThread  MMTrigger = "thread"
	MMTriggerEdit    MMTrigger = "edit"
)

type MMNormalizedFile struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	MIME string `json:"mime,omitempty"`
	Size int64  `json:"size,omitempty"`
}

type MMParsedPost struct {
	Post        MMPost
	ChannelType string
	IsDM        bool
	Mentioned   bool
	Sender      string
	ChannelName string
	Team        string
	RootID      string
	Files       []MMNormalizedFile
}

type MMNormalized struct {
	Post        MMPost
	Content     string
	Meta        map[string]string
	ChannelID   string
	PostID      string
	RootID      string
	ChannelType string
	ChannelName string
	User        string
	Team        string
	Files       []MMNormalizedFile
	Trigger     MMTrigger
}

var mmChannelTypeLabel = map[string]string{
	"D": "direct message",
	"O": "public channel",
	"P": "private channel",
	"G": "group message",
}

func mmFlaggedMachine(props map[string]any) bool {
	truthy := func(value any) bool {
		if flag, ok := value.(bool); ok {
			return flag
		}
		text, _ := value.(string)
		return text == "true"
	}
	return truthy(props["from_bot"]) || truthy(props["from_webhook"])
}

func ExtractMMFiles(post MMPost) []MMNormalizedFile {
	metadata := make(map[string]MMFileMeta, len(post.Metadata.Files))
	for _, file := range post.Metadata.Files {
		metadata[file.ID] = file
	}
	ids := post.FileIDs
	if len(ids) == 0 {
		ids = make([]string, 0, len(post.Metadata.Files))
		for _, file := range post.Metadata.Files {
			ids = append(ids, file.ID)
		}
	}
	files := make([]MMNormalizedFile, 0, len(ids))
	for _, id := range ids {
		meta := metadata[id]
		files = append(files, MMNormalizedFile{ID: id, Name: meta.Name, MIME: meta.MIMEType, Size: meta.Size})
	}
	return files
}

func ParseMMPost(event MMEvent, botUserID string) *MMParsedPost {
	if event.Event != "posted" || event.Data == nil || event.Data.Post == "" {
		return nil
	}
	var post MMPost
	if err := json.Unmarshal([]byte(event.Data.Post), &post); err != nil || post.ID == "" {
		return nil
	}
	if post.UserID == botUserID || mmFlaggedMachine(post.Props) {
		return nil
	}
	var mentions []string
	if event.Data.Mentions != "" {
		_ = json.Unmarshal([]byte(event.Data.Mentions), &mentions)
	}
	mentioned := false
	for _, id := range mentions {
		if id == botUserID {
			mentioned = true
			break
		}
	}
	rootID := post.RootID
	if rootID == "" {
		rootID = post.ID
	}
	channelName := event.Data.ChannelName
	if channelName == "" {
		channelName = event.Data.ChannelDisplayName
	}
	return &MMParsedPost{
		Post:        post,
		ChannelType: event.Data.ChannelType,
		IsDM:        event.Data.ChannelType == "D",
		Mentioned:   mentioned,
		Sender:      strings.TrimPrefix(event.Data.SenderName, "@"),
		ChannelName: channelName,
		Team:        event.Data.TeamID,
		RootID:      rootID,
		Files:       ExtractMMFiles(post),
	}
}

func NormalizeMMPost(event MMEvent, botUserID string, threadFollowed bool) *MMNormalized {
	parsed := ParseMMPost(event, botUserID)
	if parsed == nil {
		return nil
	}
	var trigger MMTrigger
	switch {
	case parsed.IsDM:
		trigger = MMTriggerDM
	case parsed.Mentioned:
		trigger = MMTriggerMention
	case threadFollowed:
		trigger = MMTriggerThread
	default:
		return nil
	}
	where := mmChannelTypeLabel[parsed.ChannelType]
	if where == "" {
		where = parsed.ChannelType
		if where == "" {
			where = "unknown"
		}
	}
	who := parsed.Sender
	if who == "" {
		who = "someone"
	}
	var header string
	switch trigger {
	case MMTriggerDM:
		header = "New Mattermost DM from " + who + ":"
	case MMTriggerMention:
		place := parsed.ChannelName
		if place == "" {
			place = "a " + where
		}
		header = "New Mattermost @mention from " + who + " in " + place + ":"
	default:
		place := parsed.ChannelName
		if place == "" {
			place = "a " + where
		}
		header = "New Mattermost thread reply from " + who + " in " + place + " (a thread you're part of):"
	}
	body := strings.TrimSpace(parsed.Post.Message)
	if body == "" {
		if len(parsed.Files) > 0 {
			suffix := ""
			if len(parsed.Files) > 1 {
				suffix = "s"
			}
			body = fmt.Sprintf("(no text — %d attachment%s)", len(parsed.Files), suffix)
		} else {
			body = "(empty message)"
		}
	}
	meta := map[string]string{
		"source":       "mattermost-channel",
		"type":         "post",
		"channel_type": parsed.ChannelType,
		"channel_id":   parsed.Post.ChannelID,
		"post_id":      parsed.Post.ID,
		"root_id":      parsed.RootID,
		"trigger":      string(trigger),
	}
	mmPutMeta(meta, "user", parsed.Sender)
	mmPutMeta(meta, "team", parsed.Team)
	mmPutMeta(meta, "channel_name", parsed.ChannelName)
	if len(parsed.Files) > 0 {
		meta["attachments"] = fmt.Sprint(len(parsed.Files))
	}
	return &MMNormalized{
		Post: parsed.Post, Content: header + "\n\n" + body, Meta: meta,
		ChannelID: parsed.Post.ChannelID, PostID: parsed.Post.ID, RootID: parsed.RootID,
		ChannelType: parsed.ChannelType, ChannelName: parsed.ChannelName, User: parsed.Sender,
		Team: parsed.Team, Files: parsed.Files, Trigger: trigger,
	}
}

func MMInThread(post MMPost) bool {
	return post.RootID != "" && post.RootID != post.ID
}

func MMTypingParentID(normalized MMNormalized) string {
	if normalized.ChannelType == "D" || !MMInThread(normalized.Post) {
		return ""
	}
	return normalized.RootID
}

type MMPostEventKind string

const (
	MMPostEdited  MMPostEventKind = "post_edited"
	MMPostDeleted MMPostEventKind = "post_deleted"
)

func ParseMMPostEvent(event MMEvent, kind MMPostEventKind, botUserID string) *MMPost {
	if event.Event != string(kind) || event.Data == nil || event.Data.Post == "" {
		return nil
	}
	var post MMPost
	if err := json.Unmarshal([]byte(event.Data.Post), &post); err != nil || post.ID == "" {
		return nil
	}
	if post.UserID == botUserID || mmFlaggedMachine(post.Props) {
		return nil
	}
	return &post
}

func MMIsGenuineEdit(post MMPost, knownEditAt *int64) bool {
	if post.EditAt <= 0 {
		return false
	}
	return knownEditAt == nil || *knownEditAt != post.EditAt
}

func MMEditEventKey(post MMPost) string {
	return fmt.Sprintf("%s:edit:%d", post.ID, post.EditAt)
}

func MMDeleteEventKey(post MMPost) string { return post.ID + ":delete" }

type MMMutationContext struct {
	ChannelType     string
	ChannelName     string
	Team            string
	Sender          string
	PreviousMessage *string
	Trigger         MMTrigger
}

func MMFormatEditDiff(before, after string) string {
	show := func(value string) string {
		if strings.TrimSpace(value) == "" {
			return "(empty message)"
		}
		return value
	}
	return "--- before (as it was delivered to you) ---\n" + show(before) +
		"\n--- after (current) ---\n" + show(after)
}

const mmEditACKDirective = "ACKNOWLEDGE THIS IN THE CHANNEL: reply with `chat_reply` (the host threads it on root_id " +
	"outside a DM) briefly noting that the message was edited and what you are doing about " +
	"it — re-answering, revising an earlier reply, or confirming nothing changes. The human " +
	"edited it expecting to see that you noticed."

const mmDeleteACKDirective = "ACKNOWLEDGE THIS IN THE CHANNEL: reply with `chat_reply` (the host threads it on root_id " +
	"outside a DM) noting the message was withdrawn and whether that changes anything you " +
	"already said or did. Do NOT quote the deleted text back in full — it was withdrawn."

func mmMutationMeta(post MMPost, context MMMutationContext, eventType string) map[string]string {
	rootID := post.RootID
	if rootID == "" {
		rootID = post.ID
	}
	meta := map[string]string{
		"source":       "mattermost-channel",
		"type":         eventType,
		"channel_type": context.ChannelType,
		"channel_id":   post.ChannelID,
		"post_id":      post.ID,
		"root_id":      rootID,
		"trigger":      string(context.Trigger),
		"seen_before":  fmt.Sprint(context.PreviousMessage != nil),
	}
	mmPutMeta(meta, "user", context.Sender)
	mmPutMeta(meta, "team", context.Team)
	mmPutMeta(meta, "channel_name", context.ChannelName)
	return meta
}

func NormalizeMMEdit(post MMPost, context MMMutationContext) MMNormalized {
	where := mmPlace(context.ChannelType, context.ChannelName)
	who := context.Sender
	if who == "" {
		who = "someone"
	}
	var header, body string
	if context.PreviousMessage != nil {
		header = "Mattermost message EDITED by " + who + " in " + where + " — you were sent this post earlier, and its text has since changed."
		body = MMFormatEditDiff(*context.PreviousMessage, post.Message)
	} else {
		header = fmt.Sprintf("Mattermost message EDITED by %s in %s — the edit is what brought it to you (trigger=%q); you had NOT been sent this post before, so there is no \"before\" to compare.", who, where, context.Trigger)
		body = strings.TrimSpace(post.Message)
		if body == "" {
			body = "(empty message)"
		}
	}
	files := ExtractMMFiles(post)
	meta := mmMutationMeta(post, context, "post_edited")
	if len(files) > 0 {
		meta["attachments"] = fmt.Sprint(len(files))
	}
	if post.EditAt != 0 {
		meta["edited_at"] = mmISOTime(post.EditAt)
	}
	rootID := post.RootID
	if rootID == "" {
		rootID = post.ID
	}
	return MMNormalized{
		Post: post, Content: header + "\n\n" + body + "\n\n" + mmEditACKDirective, Meta: meta,
		ChannelID: post.ChannelID, PostID: post.ID, RootID: rootID, ChannelType: context.ChannelType,
		ChannelName: context.ChannelName, User: context.Sender, Team: context.Team, Files: files, Trigger: context.Trigger,
	}
}

func NormalizeMMDelete(post MMPost, context MMMutationContext) MMNormalized {
	where := mmPlace(context.ChannelType, context.ChannelName)
	who := context.Sender
	if who == "" {
		who = "someone"
	}
	text := post.Message
	if context.PreviousMessage != nil {
		text = *context.PreviousMessage
	}
	body := strings.TrimSpace(text)
	if body == "" {
		body = "(empty message)"
	}
	header := "Mattermost message DELETED in " + where + " — you were sent this post earlier (from " + who + "). It no longer exists in Mattermost."
	meta := mmMutationMeta(post, context, "post_deleted")
	if post.DeleteAt != 0 {
		meta["deleted_at"] = mmISOTime(post.DeleteAt)
	}
	rootID := post.RootID
	if rootID == "" {
		rootID = post.ID
	}
	return MMNormalized{
		Post: post, Content: header + "\n\n--- the post as it was delivered to you ---\n" + body + "\n\n" + mmDeleteACKDirective,
		Meta: meta, ChannelID: post.ChannelID, PostID: post.ID, RootID: rootID, ChannelType: context.ChannelType,
		ChannelName: context.ChannelName, User: context.Sender, Team: context.Team, Trigger: context.Trigger,
	}
}

var mmMentionPattern = regexp.MustCompile(`(?:^|[^\w.@\-])@([\w.\-]+)`)

func MMMentions(message, username string) bool {
	if message == "" || username == "" || username == "the bot" {
		return false
	}
	target := strings.ToLower(username)
	for _, match := range mmMentionPattern.FindAllStringSubmatch(message, -1) {
		handle := strings.ToLower(match[1])
		for handle != "" {
			if handle == target {
				return true
			}
			last := handle[len(handle)-1]
			if last != '.' && last != '-' && last != '_' {
				break
			}
			handle = handle[:len(handle)-1]
		}
	}
	return false
}

type MMPriorPost struct {
	IsBot   bool
	Message string
}

func MMBotParticipates(posts []MMPriorPost, botUsername string) bool {
	for _, post := range posts {
		if post.IsBot || MMMentions(post.Message, botUsername) {
			return true
		}
	}
	return false
}

type MMReplayChannel struct {
	ID          string
	Type        string
	Name        string
	DisplayName string
	TeamID      string
}

func MMPostedCreateAt(event MMEvent) *int64 {
	if event.Event != "posted" || event.Data == nil || event.Data.Post == "" {
		return nil
	}
	var post MMPost
	if err := json.Unmarshal([]byte(event.Data.Post), &post); err != nil {
		return nil
	}
	value := post.CreateAt
	return &value
}

func MMReplayEvent(post MMPost, channel MMReplayChannel, senderUsername, botUserID, botUsername string) MMEvent {
	data := &MMPostedData{
		ChannelType: channel.Type, ChannelName: channel.Name, ChannelDisplayName: channel.DisplayName,
		TeamID: channel.TeamID, Post: mmMustJSON(post),
	}
	if senderUsername != "" {
		data.SenderName = "@" + senderUsername
	}
	if channel.Type != "D" && MMMentions(post.Message, botUsername) {
		data.Mentions = mmMustJSON([]string{botUserID})
	}
	return MMEvent{Event: "posted", Data: data}
}

func MMMutationEvent(post MMPost, kind MMPostEventKind) MMEvent {
	return MMEvent{Event: string(kind), Data: &MMPostedData{Post: mmMustJSON(post)}}
}

type MMHistoryItem struct {
	PostID  string             `json:"post_id"`
	User    string             `json:"user"`
	IsBot   bool               `json:"is_bot"`
	At      string             `json:"at"`
	Message string             `json:"message"`
	Files   []MMNormalizedFile `json:"files"`
}

type MMSavedAttachment struct {
	Name    string
	Path    string
	MIME    string
	IsImage bool
	Note    string
}

func MMFormatAttachmentLine(attachment MMSavedAttachment) string {
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

func MMFormatAttachmentBlock(saved []MMSavedAttachment) string {
	lines := make([]string, len(saved))
	for i, attachment := range saved {
		lines[i] = MMFormatAttachmentLine(attachment)
	}
	return fmt.Sprintf("--- attachments (%d) ---\n%s", len(saved), strings.Join(lines, "\n"))
}

func MMFormatContextBlock(items []MMHistoryItem, label, botUsername string, attachments map[string][]MMSavedAttachment) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		who := "@" + item.User
		if item.IsBot {
			who = "@" + botUsername + " (you)"
		}
		lines = append(lines, fmt.Sprintf("[%s] %s: %s", item.At, who, item.Message))
		for _, attachment := range attachments[item.PostID] {
			lines = append(lines, "    "+MMFormatAttachmentLine(attachment))
		}
	}
	return fmt.Sprintf("--- %s (oldest first, %d post(s)) ---\n%s", label, len(items), strings.Join(lines, "\n"))
}

func mmPlace(channelType, channelName string) string {
	if channelName != "" {
		return channelName
	}
	where := mmChannelTypeLabel[channelType]
	if where == "" {
		where = channelType
		if where == "" {
			where = "unknown"
		}
	}
	return "a " + where
}

func mmISOTime(milliseconds int64) string {
	return time.UnixMilli(milliseconds).UTC().Format("2006-01-02T15:04:05.000Z")
}

func mmPutMeta(meta map[string]string, key, value string) {
	if value != "" {
		meta[key] = value
	}
}

func mmMustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func mmSortPosts(posts []MMPost) {
	sort.Slice(posts, func(i, j int) bool { return posts[i].CreateAt < posts[j].CreateAt })
}
