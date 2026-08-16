package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	mmTestBot    = "bot-user-id"
	mmTestTarget = "agent-a"
)

type mmTestAPI struct {
	mu sync.Mutex

	server  *httptest.Server
	posts   []MMCreatePost
	postErr bool

	channelPosts map[string]MMPostList
	threads      map[string]MMPostList
	since        map[string]MMPostList
	channels     map[string]MMChannel
	users        map[string]string
	files        map[string][]byte
	fileInfo     map[string]MMFileInfo

	allChannelsErr bool
	fileReads      int
	lastPerPage    int
}

func mmNewTestAPI(t *testing.T) *mmTestAPI {
	t.Helper()
	api := &mmTestAPI{
		channelPosts: make(map[string]MMPostList), threads: make(map[string]MMPostList),
		since: make(map[string]MMPostList), channels: make(map[string]MMChannel),
		users: map[string]string{"u1": "alice", mmTestBot: "courierbot"},
		files: make(map[string][]byte), fileInfo: make(map[string]MMFileInfo),
	}
	api.server = httptest.NewServer(http.HandlerFunc(api.serveHTTP))
	t.Cleanup(api.server.Close)
	return api
}

func (a *mmTestAPI) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Authorization") != "Bearer token" {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/api/v4")
	a.mu.Lock()
	defer a.mu.Unlock()
	write := func(status int, value any) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(value)
	}
	switch {
	case request.Method == http.MethodGet && path == "/users/me":
		write(http.StatusOK, MMUser{ID: mmTestBot, Username: "courierbot"})
	case request.Method == http.MethodPost && path == "/posts":
		if a.postErr {
			http.Error(writer, "down", http.StatusInternalServerError)
			return
		}
		var post MMCreatePost
		if err := json.NewDecoder(request.Body).Decode(&post); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		a.posts = append(a.posts, post)
		write(http.StatusCreated, MMCreatedPost{ID: fmt.Sprintf("posted-%d", len(a.posts)), ChannelID: post.ChannelID, RootID: post.RootID, Message: post.Message, CreateAt: 1})
	case request.Method == http.MethodPost && path == "/users/ids":
		var ids []string
		_ = json.NewDecoder(request.Body).Decode(&ids)
		users := make([]MMUser, len(ids))
		for i, id := range ids {
			name := a.users[id]
			if name == "" {
				name = id
			}
			users[i] = MMUser{ID: id, Username: name}
		}
		write(http.StatusOK, users)
	case request.Method == http.MethodGet && path == "/users/me/channels":
		if a.allChannelsErr {
			http.Error(writer, "unsupported", http.StatusNotFound)
			return
		}
		channels := make([]MMChannel, 0, len(a.channels))
		for _, channel := range a.channels {
			channels = append(channels, channel)
		}
		write(http.StatusOK, channels)
	case request.Method == http.MethodGet && path == "/users/me/teams":
		write(http.StatusOK, []MMTeam{{ID: "team"}})
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/users/me/teams/") && strings.HasSuffix(path, "/channels"):
		channels := make([]MMChannel, 0, len(a.channels))
		for _, channel := range a.channels {
			channels = append(channels, channel)
		}
		write(http.StatusOK, channels)
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/posts/") && strings.HasSuffix(path, "/thread"):
		postID := strings.TrimSuffix(strings.TrimPrefix(path, "/posts/"), "/thread")
		write(http.StatusOK, mmTestList(a.threads[postID]))
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/channels/") && strings.HasSuffix(path, "/posts"):
		channelID := strings.TrimSuffix(strings.TrimPrefix(path, "/channels/"), "/posts")
		if perPage := request.URL.Query().Get("per_page"); perPage != "" {
			a.lastPerPage, _ = strconv.Atoi(perPage)
		}
		if request.URL.Query().Has("since") {
			write(http.StatusOK, mmTestList(a.since[channelID]))
		} else {
			write(http.StatusOK, mmTestList(a.channelPosts[channelID]))
		}
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/channels/"):
		channelID := strings.TrimPrefix(path, "/channels/")
		channel, found := a.channels[channelID]
		if !found {
			http.Error(writer, "missing channel", http.StatusNotFound)
			return
		}
		write(http.StatusOK, channel)
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/files/") && strings.HasSuffix(path, "/info"):
		fileID := strings.TrimSuffix(strings.TrimPrefix(path, "/files/"), "/info")
		info, found := a.fileInfo[fileID]
		if !found {
			http.Error(writer, "missing file", http.StatusNotFound)
			return
		}
		write(http.StatusOK, info)
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/files/"):
		fileID := strings.TrimPrefix(path, "/files/")
		content, found := a.files[fileID]
		if !found {
			http.Error(writer, "missing file", http.StatusNotFound)
			return
		}
		a.fileReads++
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(content)
	default:
		http.Error(writer, "unhandled "+request.Method+" "+path, http.StatusNotFound)
	}
}

func mmTestList(list MMPostList) MMPostList {
	if list.Posts == nil {
		list.Posts = map[string]MMPost{}
	}
	return list
}

type mmTestRig struct {
	store     *Store
	api       *mmTestAPI
	connector *MattermostConnector
	clock     *int64
	dir       string
}

func mmNewTestRig(t *testing.T, mutate ...func(*MattermostConnectorConfig)) *mmTestRig {
	t.Helper()
	directory := t.TempDir()
	store, err := Open(filepath.Join(directory, "courier.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	api := mmNewTestAPI(t)
	client, err := NewMattermostClient(api.server.URL, "token", api.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	now := int64(5_000_000)
	config := MattermostConnectorConfig{
		Store: store, Target: mmTestTarget,
		Options: MattermostOptions{URL: api.server.URL, BotToken: "token", AttachmentDir: filepath.Join(directory, "attachments"), BotUserID: mmTestBot},
		Client:  client, Now: func() int64 { return now },
	}
	for _, change := range mutate {
		change(&config)
	}
	connector, err := NewMattermostConnector(config)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	rig := &mmTestRig{store: store, api: api, connector: connector, clock: &now, dir: directory}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = connector.Stop(ctx)
		_ = store.Close()
	})
	return rig
}

func mmTestPost(overrides ...func(*MMPost)) MMPost {
	post := MMPost{ID: "p1", ChannelID: "c1", UserID: "u1", Message: "hello", CreateAt: 1000}
	for _, change := range overrides {
		change(&post)
	}
	return post
}

func mmTestPosted(post MMPost, channelType string, mutate ...func(*MMPostedData)) (MMEvent, string) {
	data := &MMPostedData{ChannelType: channelType, SenderName: "@alice", Post: mmMustJSON(post)}
	for _, change := range mutate {
		change(data)
	}
	event := MMEvent{Event: "posted", Data: data}
	return event, mmMustJSON(event)
}

func mmTestMutation(post MMPost, kind MMPostEventKind) (MMEvent, string) {
	event := MMMutationEvent(post, kind)
	return event, mmMustJSON(event)
}

func mmTestFindEvent(t *testing.T, store *Store, key string) *Event {
	t.Helper()
	event, err := store.FindEvent(MattermostName, key)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func mmTestDeliveryContext(t *testing.T, store *Store, event *Event) DeliveryContext {
	t.Helper()
	delivery, err := store.OpenDeliveryForEvent(event.ID)
	if err != nil || delivery == nil {
		t.Fatalf("OpenDeliveryForEvent = %#v, %v", delivery, err)
	}
	return DeliveryContext{Delivery: *delivery, Event: *event, ConversationID: event.ConversationID}
}

func TestMMConversationIDTopLevel(t *testing.T) {
	if got := MMConversationIDFor(MMPost{ID: "p1", RootID: "", ChannelID: "c1"}); got != "c1" {
		t.Fatalf("empty-root conversation = %q", got)
	}
	if got := MMConversationIDFor(MMPost{ID: "p1", RootID: "p1", ChannelID: "c1"}); got != "c1" {
		t.Fatalf("self-root conversation = %q", got)
	}
}

func TestMMConversationIDThread(t *testing.T) {
	if got := MMConversationIDFor(MMPost{ID: "p2", RootID: "p1", ChannelID: "c1"}); got != "c1:p1" {
		t.Fatalf("thread conversation = %q", got)
	}
}

func TestMMConversationDecomposeFirstColon(t *testing.T) {
	channel, root := MMDecomposeConversationID("c1")
	if channel != "c1" || root != "" {
		t.Fatalf("flat decomposition = %q %q", channel, root)
	}
	channel, root = MMDecomposeConversationID("c1:root:remainder")
	if channel != "c1" || root != "root:remainder" {
		t.Fatalf("first-colon decomposition = %q %q", channel, root)
	}
}

func TestMMDMIngestsEventAndDelivery(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "D")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "p1")
	if stored == nil || stored.ConversationID != "c1" || stored.User == nil || *stored.User != "alice" || !strings.Contains(stored.Content, "New Mattermost DM from alice") {
		t.Fatalf("stored event = %#v", stored)
	}
	if delivery, _ := rig.store.OpenDeliveryForEvent(stored.ID); delivery == nil || delivery.Target != mmTestTarget {
		t.Fatalf("delivery = %#v", delivery)
	}
}

func TestMMUnmentionedChannelPostDrops(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "O")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	if count, _ := rig.store.CountEvents(); count != 0 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMChannelMentionIngests(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "O", func(data *MMPostedData) {
		data.ChannelName = "general"
		data.Mentions = mmMustJSON([]string{mmTestBot})
	})
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	if stored := mmTestFindEvent(t, rig.store, "p1"); stored == nil || stored.ConversationID != "c1" {
		t.Fatalf("mention event = %#v", stored)
	}
}

func TestMMLoopPreventionDropsOwnAndMachinePosts(t *testing.T) {
	for name, mutate := range map[string]func(*MMPost){
		"own":          func(post *MMPost) { post.UserID = mmTestBot },
		"from_bot":     func(post *MMPost) { post.Props = map[string]any{"from_bot": true} },
		"from_webhook": func(post *MMPost) { post.Props = map[string]any{"from_webhook": "true"} },
	} {
		t.Run(name, func(t *testing.T) {
			rig := mmNewTestRig(t)
			post := mmTestPost(mutate)
			event, raw := mmTestPosted(post, "D")
			if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
				t.Fatal(err)
			}
			if count, _ := rig.store.CountEvents(); count != 0 {
				t.Fatalf("event count = %d", count)
			}
		})
	}
}

func TestMMFollowedThreadIngestsWithoutFreshMention(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"root1"}, Posts: map[string]MMPost{
		"root1": {ID: "root1", CreateAt: 900, UserID: mmTestBot, Message: "earlier, from me"},
	}}
	post := mmTestPost(func(post *MMPost) { post.ID, post.RootID, post.ChannelID = "p2", "root1", "c2" })
	event, raw := mmTestPosted(post, "O")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "p2")
	if stored == nil || stored.ConversationID != "c2:root1" || !strings.Contains(stored.Content, "thread reply") {
		t.Fatalf("thread event = %#v", stored)
	}
}

func TestMMThreadWithoutFetchedParticipationDrops(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"root1"}, Posts: map[string]MMPost{
		"root1": {ID: "root1", CreateAt: 900, UserID: "u1", Message: "no bot here"},
	}}
	post := mmTestPost(func(post *MMPost) { post.ID, post.RootID = "p2", "root1" })
	event, raw := mmTestPosted(post, "O")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	if count, _ := rig.store.CountEvents(); count != 0 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMTriggerPrecedenceDMThenMentionThenThread(t *testing.T) {
	base := MMEvent{Event: "posted", Data: &MMPostedData{ChannelType: "D", Mentions: mmMustJSON([]string{mmTestBot}), Post: mmMustJSON(mmTestPost())}}
	if got := NormalizeMMPost(base, mmTestBot, true); got == nil || got.Trigger != MMTriggerDM {
		t.Fatalf("DM precedence = %#v", got)
	}
	base.Data.ChannelType = "O"
	if got := NormalizeMMPost(base, mmTestBot, true); got == nil || got.Trigger != MMTriggerMention {
		t.Fatalf("mention precedence = %#v", got)
	}
	base.Data.Mentions = "[]"
	if got := NormalizeMMPost(base, mmTestBot, true); got == nil || got.Trigger != MMTriggerThread {
		t.Fatalf("thread fallback = %#v", got)
	}
}

func TestMMInsideThreadPredicate(t *testing.T) {
	if MMInThread(MMPost{ID: "p", RootID: ""}) || MMInThread(MMPost{ID: "p", RootID: "p"}) || !MMInThread(MMPost{ID: "p", RootID: "root"}) {
		t.Fatal("inside-thread predicate drifted")
	}
}

func TestMMDuplicateFrameIsOneEventAndDelivery(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "D")
	for i := 0; i < 2; i++ {
		if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
			t.Fatal(err)
		}
	}
	if count, _ := rig.store.CountEvents(); count != 1 {
		t.Fatalf("event count = %d", count)
	}
	if deliveries, _ := rig.store.DeliveriesForTarget(mmTestTarget); len(deliveries) != 1 {
		t.Fatalf("deliveries = %d", len(deliveries))
	}
}

func TestMMEditCarriesKeyDiffAndDirective(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	first, raw := mmTestPosted(mmTestPost(func(post *MMPost) { post.Message = "origin" }), "D")
	if err := rig.connector.HandleEvent(context.Background(), first, raw, false); err != nil {
		t.Fatal(err)
	}
	post := mmTestPost(func(post *MMPost) { post.Message, post.EditAt = "revised", 2000 })
	edited, editedRaw := mmTestMutation(post, MMPostEdited)
	if err := rig.connector.HandleEvent(context.Background(), edited, editedRaw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "p1:edit:2000")
	if stored == nil || !strings.Contains(stored.Content, "origin") || !strings.Contains(stored.Content, "revised") || !strings.Contains(stored.Content, "chat_reply") || strings.Contains(stored.Content, "mattermost_reply") {
		t.Fatalf("edit event = %#v", stored)
	}
}

func TestMMServerRewriteIsNotEdit(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	first, raw := mmTestPosted(mmTestPost(), "D")
	_ = rig.connector.HandleEvent(context.Background(), first, raw, false)
	rewrite, rewriteRaw := mmTestMutation(mmTestPost(func(post *MMPost) { post.Message = "link preview" }), MMPostEdited)
	if err := rig.connector.HandleEvent(context.Background(), rewrite, rewriteRaw, false); err != nil {
		t.Fatal(err)
	}
	if count, _ := rig.store.CountEvents(); count != 1 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMSameEditTwiceIsOneRow(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	first, raw := mmTestPosted(mmTestPost(), "D")
	_ = rig.connector.HandleEvent(context.Background(), first, raw, false)
	edited, editedRaw := mmTestMutation(mmTestPost(func(post *MMPost) { post.Message, post.EditAt = "revised", 2000 }), MMPostEdited)
	for i := 0; i < 2; i++ {
		if err := rig.connector.HandleEvent(context.Background(), edited, editedRaw, false); err != nil {
			t.Fatal(err)
		}
	}
	if count, _ := rig.store.CountEvents(); count != 2 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMDeleteCarriesKeyAndDirectiveOnce(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	first, raw := mmTestPosted(mmTestPost(func(post *MMPost) { post.Message = "withdraw me" }), "D")
	_ = rig.connector.HandleEvent(context.Background(), first, raw, false)
	deleted, deletedRaw := mmTestMutation(mmTestPost(func(post *MMPost) { post.DeleteAt = 3000 }), MMPostDeleted)
	for i := 0; i < 2; i++ {
		if err := rig.connector.HandleEvent(context.Background(), deleted, deletedRaw, false); err != nil {
			t.Fatal(err)
		}
	}
	stored := mmTestFindEvent(t, rig.store, "p1:delete")
	if stored == nil || !strings.Contains(stored.Content, "withdraw me") || !strings.Contains(stored.Content, "Do NOT quote") {
		t.Fatalf("delete event = %#v", stored)
	}
	if count, _ := rig.store.CountEvents(); count != 2 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMUnseenDeleteDrops(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	deleted, raw := mmTestMutation(mmTestPost(func(post *MMPost) { post.DeleteAt = 3000 }), MMPostDeleted)
	if err := rig.connector.HandleEvent(context.Background(), deleted, raw, false); err != nil {
		t.Fatal(err)
	}
	if count, _ := rig.store.CountEvents(); count != 0 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMCatchUpReplaysGapAndMovesWatermark(t *testing.T) {
	rig := mmNewTestRig(t)
	_ = rig.store.SetSyncState(MMWatermarkKey, 1000)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	rig.api.since["c1"] = MMPostList{Order: []string{"missed"}, Posts: map[string]MMPost{
		"missed": {ID: "missed", ChannelID: "c1", CreateAt: 2000, UserID: "u1", Message: "while you were out"},
	}}
	if err := rig.connector.RunCatchUp(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "missed")
	var meta map[string]string
	_ = json.Unmarshal([]byte(stored.MetaJSON), &meta)
	watermark, _ := rig.store.GetSyncState(MMWatermarkKey)
	if stored == nil || meta["replayed"] != "true" || watermark == nil || *watermark != 2000 {
		t.Fatalf("replay event=%#v meta=%v watermark=%v", stored, meta, watermark)
	}
}

func TestMMCatchUpLiveDuplicateDoesNotDoubleDeliver(t *testing.T) {
	rig := mmNewTestRig(t)
	_ = rig.store.SetSyncState(MMWatermarkKey, 1000)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	post := mmTestPost(func(post *MMPost) { post.ID, post.CreateAt = "both", 2000 })
	live, raw := mmTestPosted(post, "D")
	_ = rig.connector.HandleEvent(context.Background(), live, raw, false)
	rig.api.since["c1"] = MMPostList{Order: []string{"both"}, Posts: map[string]MMPost{"both": post}}
	if err := rig.connector.RunCatchUp(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	if count, _ := rig.store.CountEvents(); count != 1 {
		t.Fatalf("event count = %d", count)
	}
}

func TestMMCatchUpEditDoesNotRewindWatermark(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.channels["c1"] = MMChannel{ID: "c1", Type: "D"}
	firstPost := mmTestPost(func(post *MMPost) { post.CreateAt = 500 })
	first, raw := mmTestPosted(firstPost, "D")
	_ = rig.connector.HandleEvent(context.Background(), first, raw, false)
	_ = rig.store.SetSyncState(MMWatermarkKey, 1000)
	firstPost.Message, firstPost.EditAt = "edited while away", 4000
	rig.api.since["c1"] = MMPostList{Order: []string{"p1"}, Posts: map[string]MMPost{"p1": firstPost}}
	if err := rig.connector.RunCatchUp(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	watermark, _ := rig.store.GetSyncState(MMWatermarkKey)
	if mmTestFindEvent(t, rig.store, "p1:edit:4000") == nil || watermark == nil || *watermark != 1000 {
		t.Fatalf("watermark = %v", watermark)
	}
}

func TestMMPostReplyUnthreaded(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "D")
	_ = rig.connector.HandleEvent(context.Background(), event, raw, false)
	stored := mmTestFindEvent(t, rig.store, "p1")
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, stored), " answered "); err != nil {
		t.Fatal(err)
	}
	rig.api.mu.Lock()
	defer rig.api.mu.Unlock()
	if len(rig.api.posts) != 1 || rig.api.posts[0].ChannelID != "c1" || rig.api.posts[0].RootID != "" || rig.api.posts[0].Message != "answered" {
		t.Fatalf("posts = %#v", rig.api.posts)
	}
}

func TestMMPostReplyThreaded(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"root1"}, Posts: map[string]MMPost{"root1": {ID: "root1", UserID: mmTestBot, CreateAt: 1}}}
	post := mmTestPost(func(post *MMPost) { post.ID, post.RootID, post.ChannelID = "p2", "root1", "c2" })
	event, raw := mmTestPosted(post, "O")
	_ = rig.connector.HandleEvent(context.Background(), event, raw, false)
	stored := mmTestFindEvent(t, rig.store, "p2")
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, stored), "answered"); err != nil {
		t.Fatal(err)
	}
	rig.api.mu.Lock()
	defer rig.api.mu.Unlock()
	if len(rig.api.posts) != 1 || rig.api.posts[0].ChannelID != "c2" || rig.api.posts[0].RootID != "root1" {
		t.Fatalf("posts = %#v", rig.api.posts)
	}
}

func TestMMPostReplyFailureLeavesUnhandled(t *testing.T) {
	rig := mmNewTestRig(t)
	event, raw := mmTestPosted(mmTestPost(), "D")
	_ = rig.connector.HandleEvent(context.Background(), event, raw, false)
	stored := mmTestFindEvent(t, rig.store, "p1")
	rig.api.mu.Lock()
	rig.api.postErr = true
	rig.api.mu.Unlock()
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, stored), "answered"); err == nil {
		t.Fatal("failed post returned nil")
	}
	stored, _ = rig.store.GetEvent(stored.ID)
	if stored.HandledAt != nil {
		t.Fatal("failed post handled event")
	}
}

func TestMMPostReplyRejectsEmpty(t *testing.T) {
	rig := mmNewTestRig(t)
	if err := rig.connector.PostReply(context.Background(), DeliveryContext{ConversationID: "c1"}, "   "); err == nil {
		t.Fatal("empty reply accepted")
	}
	rig.api.mu.Lock()
	defer rig.api.mu.Unlock()
	if len(rig.api.posts) != 0 {
		t.Fatalf("posts = %#v", rig.api.posts)
	}
}

func TestMMAutoHandleDMConversation(t *testing.T) {
	rig := mmNewTestRig(t)
	for _, id := range []string{"p1", "p2", "p3"} {
		event, raw := mmTestPosted(mmTestPost(func(post *MMPost) { post.ID = id }), "D")
		_ = rig.connector.HandleEvent(context.Background(), event, raw, false)
	}
	foreign, foreignRaw := mmTestPosted(mmTestPost(func(post *MMPost) {
		post.ID, post.ChannelID = "foreign", "other-channel"
	}), "D")
	_ = rig.connector.HandleEvent(context.Background(), foreign, foreignRaw, false)
	target := mmTestFindEvent(t, rig.store, "p3")
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, target), "all three"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p1", "p2", "p3"} {
		stored := mmTestFindEvent(t, rig.store, id)
		if stored.HandledAt == nil {
			t.Errorf("%s remains unhandled", id)
		}
	}
	if mmTestFindEvent(t, rig.store, "foreign").HandledAt != nil {
		t.Fatal("conversation reply silenced a foreign channel")
	}
}

func TestMMAutoHandleExcludesLaterPost(t *testing.T) {
	rig := mmNewTestRig(t)
	first, raw := mmTestPosted(mmTestPost(), "D")
	_ = rig.connector.HandleEvent(context.Background(), first, raw, false)
	firstStored := mmTestFindEvent(t, rig.store, "p1")
	*rig.clock += 1000
	later, laterRaw := mmTestPosted(mmTestPost(func(post *MMPost) { post.ID = "p2" }), "D")
	_ = rig.connector.HandleEvent(context.Background(), later, laterRaw, false)
	*rig.clock -= 500
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, firstStored), "ok"); err != nil {
		t.Fatal(err)
	}
	firstStored, _ = rig.store.GetEvent(firstStored.ID)
	if firstStored.HandledAt == nil || mmTestFindEvent(t, rig.store, "p2").HandledAt != nil {
		t.Fatal("reply cutoff handled the wrong events")
	}
}

func TestMMAutoHandleChannelDoesNotSilenceThread(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"root1"}, Posts: map[string]MMPost{"root1": {ID: "root1", UserID: mmTestBot, CreateAt: 1}}}
	top, topRaw := mmTestPosted(mmTestPost(func(post *MMPost) { post.ChannelID = "c2" }), "O", func(data *MMPostedData) { data.Mentions = mmMustJSON([]string{mmTestBot}) })
	_ = rig.connector.HandleEvent(context.Background(), top, topRaw, false)
	thread, threadRaw := mmTestPosted(mmTestPost(func(post *MMPost) { post.ID, post.ChannelID, post.RootID = "p2", "c2", "root1" }), "O")
	_ = rig.connector.HandleEvent(context.Background(), thread, threadRaw, false)
	topStored := mmTestFindEvent(t, rig.store, "p1")
	if err := rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, topStored), "ok"); err != nil {
		t.Fatal(err)
	}
	if mmTestFindEvent(t, rig.store, "p2").HandledAt != nil {
		t.Fatal("channel reply silenced thread")
	}
}

func TestMMAutoHandleFailedPostConfirmsNothing(t *testing.T) {
	rig := mmNewTestRig(t)
	for _, id := range []string{"p1", "p2"} {
		event, raw := mmTestPosted(mmTestPost(func(post *MMPost) { post.ID = id }), "D")
		_ = rig.connector.HandleEvent(context.Background(), event, raw, false)
	}
	rig.api.mu.Lock()
	rig.api.postErr = true
	rig.api.mu.Unlock()
	target := mmTestFindEvent(t, rig.store, "p2")
	_ = rig.connector.PostReply(context.Background(), mmTestDeliveryContext(t, rig.store, target), "x")
	for _, id := range []string{"p1", "p2"} {
		if mmTestFindEvent(t, rig.store, id).HandledAt != nil {
			t.Fatalf("%s handled after failed post", id)
		}
	}
}

func TestMMHistoryThreadOldestFirst(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"b", "a"}, Posts: map[string]MMPost{
		"a": {ID: "a", CreateAt: 100, UserID: "u1", Message: "first"},
		"b": {ID: "b", CreateAt: 200, UserID: mmTestBot, Message: "second", RootID: "a"},
	}}
	result, err := rig.connector.CallTool(context.Background(), "mattermost_history", map[string]any{"root_id": "root1"})
	if err != nil {
		t.Fatal(err)
	}
	var items []MMHistoryItem
	if err := json.Unmarshal([]byte(result.Text), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].PostID != "a" || items[0].User != "alice" || !items[1].IsBot {
		t.Fatalf("history = %#v", items)
	}
}

func TestMMHistoryAcceptsConversationID(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root1"] = MMPostList{Order: []string{"a"}, Posts: map[string]MMPost{"a": {ID: "a", UserID: "u1", Message: "first"}}}
	result, err := rig.connector.CallTool(context.Background(), "mattermost_history", map[string]any{"channel_id": "c2:root1"})
	if err != nil || result.IsError || !strings.Contains(result.Text, "first") {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestMMHistoryRequiresConversation(t *testing.T) {
	rig := mmNewTestRig(t)
	result, err := rig.connector.CallTool(context.Background(), "mattermost_history", nil)
	if err != nil || !result.IsError || result.Status != 400 {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestMMAttachmentSavedAndLocalPathIncluded(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.fileInfo["f1"] = MMFileInfo{ID: "f1", Name: "screen shot.png", MIMEType: "image/png", Size: 4}
	rig.api.files["f1"] = []byte("data")
	post := mmTestPost(func(post *MMPost) { post.FileIDs = []string{"f1"} })
	event, raw := mmTestPosted(post, "D")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "p1")
	path := filepath.Join(rig.dir, "attachments", "p1", "f1-screen_shot.png")
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "data" || !strings.Contains(stored.Content, path) {
		t.Fatalf("path=%s content=%q err=%v event=%s", path, content, err, stored.Content)
	}
}

func TestMMAttachmentCapSkipsDownload(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.fileInfo["huge"] = MMFileInfo{ID: "huge", Name: "huge.bin", MIMEType: "application/octet-stream", Size: mmMaxAttachmentSize + 1}
	rig.api.files["huge"] = []byte("should not fetch")
	post := mmTestPost(func(post *MMPost) { post.FileIDs = []string{"huge"} })
	event, raw := mmTestPosted(post, "D")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "p1")
	rig.api.mu.Lock()
	reads := rig.api.fileReads
	rig.api.mu.Unlock()
	if reads != 0 || !strings.Contains(stored.Content, "too large to auto-download") {
		t.Fatalf("file reads=%d content=%s", reads, stored.Content)
	}
}

func TestMMContextWholeThreadAndDMLimit(t *testing.T) {
	rig := mmNewTestRig(t)
	rig.api.threads["root"] = MMPostList{Order: []string{"trigger", "a", "b"}, Posts: map[string]MMPost{
		"a":       {ID: "a", CreateAt: 1, UserID: "u1", Message: "first"},
		"b":       {ID: "b", CreateAt: 2, UserID: mmTestBot, Message: "bot participated"},
		"trigger": {ID: "trigger", RootID: "root", ChannelID: "c", CreateAt: 3, UserID: "u1", Message: "now"},
	}}
	post := mmTestPost(func(post *MMPost) { post.ID, post.RootID, post.ChannelID = "trigger", "root", "c" })
	event, raw := mmTestPosted(post, "O")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	stored := mmTestFindEvent(t, rig.store, "trigger")
	if !strings.Contains(stored.Content, "earlier in this thread") || !strings.Contains(stored.Content, "first") || !strings.Contains(stored.Content, "bot participated") {
		t.Fatalf("thread context = %s", stored.Content)
	}

	rig2 := mmNewTestRig(t)
	rig2.api.channelPosts["c1"] = MMPostList{Order: []string{"old"}, Posts: map[string]MMPost{"old": {ID: "old", UserID: "u1", Message: "prior DM"}}}
	dm, dmRaw := mmTestPosted(mmTestPost(), "D")
	if err := rig2.connector.HandleEvent(context.Background(), dm, dmRaw, false); err != nil {
		t.Fatal(err)
	}
	rig2.api.mu.Lock()
	perPage := rig2.api.lastPerPage
	rig2.api.mu.Unlock()
	if perPage != 20 || !strings.Contains(mmTestFindEvent(t, rig2.store, "p1").Content, "prior DM") {
		t.Fatalf("DM per_page=%d", perPage)
	}
}

type mmFakeSocket struct {
	reads  chan mmSocketRead
	writes chan []byte
	closed chan struct{}
	once   sync.Once
}

func mmNewFakeSocket() *mmFakeSocket {
	return &mmFakeSocket{reads: make(chan mmSocketRead, 32), writes: make(chan []byte, 64), closed: make(chan struct{})}
}

func (s *mmFakeSocket) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, errors.New("closed")
	case read := <-s.reads:
		return read.data, read.err
	}
}

func (s *mmFakeSocket) Write(ctx context.Context, data []byte) error {
	copyOfData := append([]byte(nil), data...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return errors.New("closed")
	case s.writes <- copyOfData:
		return nil
	}
}

func (s *mmFakeSocket) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func mmWaitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func mmSocketActions(socket *mmFakeSocket) []string {
	var actions []string
	for {
		select {
		case data := <-socket.writes:
			var payload struct {
				Action string `json:"action"`
			}
			_ = json.Unmarshal(data, &payload)
			actions = append(actions, payload.Action)
		default:
			return actions
		}
	}
}

func TestMMTypingRefreshCapAndShadowGate(t *testing.T) {
	socket := mmNewFakeSocket()
	rig := mmNewTestRig(t, func(config *MattermostConnectorConfig) {
		config.SocketFactory = func(context.Context, string, string) (mmSocket, error) { return socket, nil }
		config.TypingRefresh = 5 * time.Millisecond
		config.TypingMaximum = 20 * time.Millisecond
	})
	if rig.connector.typingRefresh != 5*time.Millisecond || mmTypingRefresh != 3*time.Second || mmTypingMaximum != 90*time.Second {
		t.Fatal("typing duration defaults drifted")
	}
	if err := rig.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	socket.reads <- mmSocketRead{data: []byte(`{"event":"hello"}`)}
	event, raw := mmTestPosted(mmTestPost(), "D")
	if err := rig.connector.HandleEvent(context.Background(), event, raw, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	actions := mmSocketActions(socket)
	typingFrames := 0
	for _, action := range actions {
		if action == "user_typing" {
			typingFrames++
		}
	}
	if typingFrames < 2 {
		t.Fatalf("typing frames = %d, actions=%v", typingFrames, actions)
	}
	rig.connector.typingMu.Lock()
	active := len(rig.connector.typing)
	rig.connector.typingMu.Unlock()
	if active != 0 {
		t.Fatalf("typing remained active after cap: %d", active)
	}

	shadowSocket := mmNewFakeSocket()
	shadow := mmNewTestRig(t, func(config *MattermostConnectorConfig) {
		config.Shadow = NewShadowMode(true)
		config.SocketFactory = func(context.Context, string, string) (mmSocket, error) { return shadowSocket, nil }
	})
	if err := shadow.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	shadowSocket.reads <- mmSocketRead{data: []byte(`{"event":"hello"}`)}
	shadowEvent, shadowRaw := mmTestPosted(mmTestPost(), "D")
	if err := shadow.connector.HandleEvent(context.Background(), shadowEvent, shadowRaw, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	for _, action := range mmSocketActions(shadowSocket) {
		if action == "user_typing" {
			t.Fatal("shadow emitted typing")
		}
	}
	shadow.connector.typingMu.Lock()
	shadowActive := len(shadow.connector.typing)
	shadow.connector.typingMu.Unlock()
	if shadowActive != 0 {
		t.Fatal("shadow armed typing timer")
	}
}

func TestMMWebSocketAuthKeepaliveAndFrameIngest(t *testing.T) {
	socket := mmNewFakeSocket()
	rig := mmNewTestRig(t, func(config *MattermostConnectorConfig) {
		config.SocketFactory = func(context.Context, string, string) (mmSocket, error) { return socket, nil }
		config.Keepalive = 5 * time.Millisecond
		config.Watchdog = 100 * time.Millisecond
	})
	if err := rig.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	socket.reads <- mmSocketRead{data: []byte(`{"event":"hello"}`)}
	event, _ := mmTestPosted(mmTestPost(), "D")
	socket.reads <- mmSocketRead{data: []byte(mmMustJSON(event))}
	mmWaitFor(t, 200*time.Millisecond, func() bool { return mmTestFindEvent(t, rig.store, "p1") != nil })
	time.Sleep(12 * time.Millisecond)
	actions := mmSocketActions(socket)
	if len(actions) == 0 || actions[0] != "authentication_challenge" {
		t.Fatalf("actions = %v", actions)
	}
	foundKeepalive := false
	for _, action := range actions {
		if action == "get_statuses" {
			foundKeepalive = true
		}
	}
	if !foundKeepalive {
		t.Fatalf("keepalive missing: %v", actions)
	}
}

func TestMMWebSocketReconnectsAfterFailure(t *testing.T) {
	first := mmNewFakeSocket()
	second := mmNewFakeSocket()
	var mu sync.Mutex
	calls := 0
	rig := mmNewTestRig(t, func(config *MattermostConnectorConfig) {
		config.ReconnectInitial = time.Millisecond
		config.ReconnectMaximum = 2 * time.Millisecond
		config.SocketFactory = func(context.Context, string, string) (mmSocket, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return first, nil
			}
			return second, nil
		}
	})
	if err := rig.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.reads <- mmSocketRead{err: errors.New("network gone")}
	mmWaitFor(t, 200*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	mmWaitFor(t, 100*time.Millisecond, func() bool {
		actions := mmSocketActions(second)
		return len(actions) > 0 && actions[0] == "authentication_challenge"
	})
}

func TestMMWebSocketWatchdogForcesReconnect(t *testing.T) {
	first := mmNewFakeSocket()
	var mu sync.Mutex
	calls := 0
	var logs []string
	rig := mmNewTestRig(t, func(config *MattermostConnectorConfig) {
		config.Now = func() int64 { return time.Now().UnixMilli() }
		config.Keepalive = time.Second
		config.Watchdog = 20 * time.Millisecond
		config.ReconnectInitial = time.Millisecond
		config.ReconnectMaximum = 2 * time.Millisecond
		config.Log = func(format string, args ...any) {
			mu.Lock()
			logs = append(logs, fmt.Sprintf(format, args...))
			mu.Unlock()
		}
		config.SocketFactory = func(context.Context, string, string) (mmSocket, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return first, nil
			}
			return mmNewFakeSocket(), nil
		}
	})
	if err := rig.connector.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	mmWaitFor(t, 500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "watchdog: no websocket activity") {
		t.Fatalf("watchdog reason not logged: %s", joined)
	}
}

func TestMMClientRejectsOversizeStreamingBody(t *testing.T) {
	api := mmNewTestAPI(t)
	api.files["large"] = []byte(strings.Repeat("x", 12))
	client, err := NewMattermostClient(api.server.URL, "token", api.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	content, err := client.GetFileBytes(context.Background(), "large", 10)
	if !errors.Is(err, ErrMMAttachmentTooLarge) || content != nil {
		t.Fatalf("GetFileBytes = %d bytes, %v", len(content), err)
	}
}

func TestMMRESTErrorsAreBoundedAndDoNotExposeToken(t *testing.T) {
	api := mmNewTestAPI(t)
	api.postErr = true
	client, err := NewMattermostClient(api.server.URL, "token", api.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CreatePost(context.Background(), MMCreatePost{ChannelID: "c", Message: "x"})
	if err == nil || strings.Contains(err.Error(), "Bearer token") || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("error = %v", err)
	}
}
