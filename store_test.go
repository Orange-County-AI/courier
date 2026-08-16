package main

import (
	"strings"
	"sync"
	"testing"
)

func openTestStore(t *testing.T, options ...StoreOption) *Store {
	t.Helper()
	store, err := Open(":memory:", options...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func insertTestEvent(t *testing.T, store *Store, key, conversation string, now int64) *Event {
	t.Helper()
	event, err := store.InsertEvent(EventInsert{
		Connector:      "mattermost",
		EventKey:       key,
		ConversationID: conversation,
		Content:        key,
	}, now)
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if event == nil {
		t.Fatal("InsertEvent returned duplicate for new key")
	}
	return event
}

func insertTestDelivery(t *testing.T, store *Store, eventID int64, target string, now int64) *Delivery {
	t.Helper()
	delivery, err := store.InsertDelivery(eventID, target, now)
	if err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}
	return delivery
}

func getTestEvent(t *testing.T, store *Store, id int64) *Event {
	t.Helper()
	event, err := store.GetEvent(id)
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if event == nil {
		t.Fatalf("event %d not found", id)
	}
	return event
}

func getTestDelivery(t *testing.T, store *Store, id string) *Delivery {
	t.Helper()
	delivery, err := store.GetDelivery(id)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if delivery == nil {
		t.Fatalf("delivery %q not found", id)
	}
	return delivery
}

func TestInsertEventDeduplicatesAtUniqueIndex(t *testing.T) {
	store := openTestStore(t)
	user := "Dana"
	first, err := store.InsertEvent(EventInsert{
		Connector:      "mattermost",
		EventKey:       "post-1",
		ConversationID: "chan-1",
		User:           &user,
		Content:        "hello",
	}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.InsertEvent(EventInsert{
		Connector:      "mattermost",
		EventKey:       "post-1",
		ConversationID: "different",
		Content:        "replacement",
	}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate != nil {
		t.Fatalf("duplicate = %#v, want nil", duplicate)
	}
	if first.MetaJSON != "{}" || first.RawJSON != "{}" || first.ReceivedAt != 1000 {
		t.Fatalf("defaults/clock not preserved: %#v", first)
	}
	found, err := store.FindEvent("mattermost", "post-1")
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.Content != "hello" {
		t.Fatalf("dedupe overwrote first event: %#v", found)
	}
	count, err := store.CountEvents("mattermost")
	if err != nil || count != 1 {
		t.Fatalf("CountEvents = %d, %v; want 1", count, err)
	}
}

func TestInsertDeliveryIsIdempotentUntilFailed(t *testing.T) {
	store := openTestStore(t)
	event := insertTestEvent(t, store, "event", "conv", 100)
	first := insertTestDelivery(t, store, event.ID, "agent-a", 200)
	second := insertTestDelivery(t, store, event.ID, "agent-b", 300)
	if first.ID != second.ID || second.Target != "agent-a" || second.CreatedAt != 200 {
		t.Fatalf("existing open delivery changed: first=%#v second=%#v", first, second)
	}
	if err := store.FailDelivery(first.ID, "abandoned"); err != nil {
		t.Fatal(err)
	}
	replacement := insertTestDelivery(t, store, event.ID, "agent-b", 300)
	if replacement.ID == first.ID || replacement.Target != "agent-b" {
		t.Fatalf("failed delivery did not free event: %#v", replacement)
	}
}

func TestClaimNextUsesEventOrderAndOneAtomicUpdate(t *testing.T) {
	store := openTestStore(t)
	firstEvent := insertTestEvent(t, store, "first", "conv", 100)
	secondEvent := insertTestEvent(t, store, "second", "conv", 200)
	secondDelivery := insertTestDelivery(t, store, secondEvent.ID, "agent", 300)
	firstDelivery := insertTestDelivery(t, store, firstEvent.ID, "agent", 400)
	generation := int64(7)

	claimed, err := store.ClaimNext("agent", 500, &generation)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.Event.ID != firstEvent.ID || claimed.Delivery.ID != firstDelivery.ID {
		t.Fatalf("first claim = %#v, want event %d delivery %s", claimed, firstEvent.ID, firstDelivery.ID)
	}
	if claimed.Delivery.Status != DeliveryDispatched || claimed.Delivery.AttemptCount != 1 ||
		claimed.Delivery.LastDispatchedAt == nil || *claimed.Delivery.LastDispatchedAt != 500 ||
		claimed.Delivery.SessionGeneration == nil || *claimed.Delivery.SessionGeneration != 7 {
		t.Fatalf("claim transition incomplete: %#v", claimed.Delivery)
	}
	claimed, err = store.ClaimNext("agent", 501, nil)
	if err != nil || claimed == nil || claimed.Delivery.ID != secondDelivery.ID {
		t.Fatalf("second claim = %#v, %v", claimed, err)
	}
}

func TestConcurrentClaimsReturnEachDeliveryOnce(t *testing.T) {
	store := openTestStore(t)
	const count = 16
	for i := 0; i < count; i++ {
		event := insertTestEvent(t, store, "event-"+string(rune('a'+i)), "conv", int64(i))
		insertTestDelivery(t, store, event.ID, "agent", int64(i))
	}

	ids := make(chan string, count)
	errs := make(chan error, count)
	var workers sync.WaitGroup
	workers.Add(count)
	for i := 0; i < count; i++ {
		go func(now int64) {
			defer workers.Done()
			claimed, err := store.ClaimNext("agent", now, nil)
			if err != nil {
				errs <- err
				return
			}
			if claimed != nil {
				ids <- claimed.Delivery.ID
			}
		}(1000 + int64(i))
	}
	workers.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("ClaimNext: %v", err)
	}
	seen := make(map[string]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("delivery %s claimed twice", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("claimed %d deliveries, want %d", len(seen), count)
	}
}

func TestReleaseKeepsAttemptsAndErrorsAreClipped(t *testing.T) {
	store := openTestStore(t)
	event := insertTestEvent(t, store, "event", "conv", 100)
	delivery := insertTestDelivery(t, store, event.ID, "agent", 100)
	if _, err := store.ClaimNext("agent", 200, nil); err != nil {
		t.Fatal(err)
	}
	message := strings.Repeat("é", 2100)
	if err := store.ReleaseToPending(delivery.ID, message, 300); err != nil {
		t.Fatal(err)
	}
	got := getTestDelivery(t, store, delivery.ID)
	if got.Status != DeliveryPending || got.AttemptCount != 1 || got.LastDispatchedAt == nil || *got.LastDispatchedAt != 300 {
		t.Fatalf("release transition = %#v", got)
	}
	if got.LastError == nil || len([]rune(*got.LastError)) != 2000 {
		t.Fatalf("last_error rune count = %d, want 2000", len([]rune(*got.LastError)))
	}
}

func TestBackoffSweepReadTierAndForeverRedelivery(t *testing.T) {
	store := openTestStore(t,
		WithRedeliverGrace(100),
		WithRedeliverReadFactor(4),
		WithRedeliverMaxBackoff(800),
	)
	if got := store.BackoffMS(0, false); got != 100 {
		t.Fatalf("attempt 0 backoff = %d", got)
	}
	if got := store.BackoffMS(2, false); got != 200 {
		t.Fatalf("attempt 2 backoff = %d", got)
	}
	if got := store.BackoffMS(2, true); got != 800 {
		t.Fatalf("read attempt 2 backoff = %d", got)
	}
	if got := store.BackoffMS(1000, false); got != 800 {
		t.Fatalf("clamped backoff = %d", got)
	}

	event := insertTestEvent(t, store, "event", "conv", 0)
	delivery := insertTestDelivery(t, store, event.ID, "agent", 0)
	if _, err := store.ClaimNext("agent", 1000, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := store.SweepStuckDispatches(1099); err != nil || len(got) != 0 {
		t.Fatalf("early sweep = %v, %v", got, err)
	}
	if got, err := store.SweepStuckDispatches(1100); err != nil || len(got) != 1 || got[0] != delivery.ID {
		t.Fatalf("due sweep = %v, %v", got, err)
	}
	if _, err := store.ClaimNext("agent", 1100, nil); err != nil {
		t.Fatal(err)
	}
	if stamped, err := store.MarkRead(delivery.ID, 1101); err != nil || !stamped {
		t.Fatalf("MarkRead = %v, %v", stamped, err)
	}
	if got, err := store.SweepStuckDispatches(1899); err != nil || len(got) != 0 {
		t.Fatalf("read-tier early sweep = %v, %v", got, err)
	}
	if got, err := store.SweepStuckDispatches(1900); err != nil || len(got) != 1 {
		t.Fatalf("read-tier due sweep = %v, %v", got, err)
	}
	if _, err := store.ClaimNext("agent", 1900, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := store.SweepStuckDispatches(2700); err != nil || len(got) != 1 {
		t.Fatalf("later capped sweep stopped redelivering: %v, %v", got, err)
	}
}

func TestReclaimStaleDispatchesHonorsPromptWindow(t *testing.T) {
	store := openTestStore(t)
	oldEvent := insertTestEvent(t, store, "old", "conv", 1)
	newEvent := insertTestEvent(t, store, "new", "conv", 2)
	oldDelivery := insertTestDelivery(t, store, oldEvent.ID, "old-agent", 1)
	newDelivery := insertTestDelivery(t, store, newEvent.ID, "new-agent", 2)
	if _, err := store.ClaimNext("old-agent", 100, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext("new-agent", 900, nil); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ReclaimStaleDispatches(500, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0] != oldDelivery.ID {
		t.Fatalf("reclaimed = %v, want %s", reclaimed, oldDelivery.ID)
	}
	if getTestDelivery(t, store, newDelivery.ID).Status != DeliveryDispatched {
		t.Fatal("young in-flight prompt was reclaimed")
	}
}

func TestDeliveryStatsDistinguishMissingFromZero(t *testing.T) {
	store := openTestStore(t)
	empty, err := store.DeliveryStats("nobody", 3500)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Unread != 0 || empty.ReadUnconfirmed != 0 || empty.OldestUnreadAgeS != nil {
		t.Fatalf("empty stats = %#v", empty)
	}
	first := insertTestEvent(t, store, "first", "conv", 1000)
	second := insertTestEvent(t, store, "second", "conv", 2500)
	insertTestDelivery(t, store, first.ID, "agent", 1000)
	read := insertTestDelivery(t, store, second.ID, "agent", 2500)
	if stamped, err := store.MarkRead(read.ID, 3000); err != nil || !stamped {
		t.Fatalf("MarkRead = %v, %v", stamped, err)
	}
	stats, err := store.DeliveryStats("agent", 3500)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unread != 1 || stats.ReadUnconfirmed != 1 || stats.OldestUnreadAgeS == nil || *stats.OldestUnreadAgeS != 3 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestInsertReplyIsIndexIdempotentAndStopsRedelivery(t *testing.T) {
	store := openTestStore(t)
	event := insertTestEvent(t, store, "event", "conv", 100)
	delivery := insertTestDelivery(t, store, event.ID, "agent", 100)
	if _, err := store.ClaimNext("agent", 200, nil); err != nil {
		t.Fatal(err)
	}
	first, duplicate, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "first",
	}, 300)
	if err != nil || duplicate {
		t.Fatalf("first InsertReply = %#v, %v, %v", first, duplicate, err)
	}
	second, duplicate, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "second",
	}, 400)
	if err != nil || !duplicate || second.ID != first.ID || second.Message != "first" {
		t.Fatalf("duplicate InsertReply = %#v, %v, %v", second, duplicate, err)
	}
	if got := getTestDelivery(t, store, delivery.ID); got.Status != DeliveryReplied {
		t.Fatalf("delivery status = %s, want replied", got.Status)
	}
	if reclaimed, err := store.SweepStuckDispatches(10_000_000); err != nil || len(reclaimed) != 0 {
		t.Fatalf("replied delivery redelivered: %v, %v", reclaimed, err)
	}
}

func TestBackfillFindsOnlyUnhandledEventsWithoutOpenDelivery(t *testing.T) {
	store := openTestStore(t)
	missing := insertTestEvent(t, store, "missing", "conv", 1)
	failedEvent := insertTestEvent(t, store, "failed", "conv", 2)
	failed := insertTestDelivery(t, store, failedEvent.ID, "old", 2)
	if err := store.FailDelivery(failed.ID, "gone"); err != nil {
		t.Fatal(err)
	}
	openEvent := insertTestEvent(t, store, "open", "conv", 3)
	insertTestDelivery(t, store, openEvent.ID, "agent", 3)
	handled := insertTestEvent(t, store, "handled", "conv", 4)
	if !store.MarkHandled(MarkHandledArgs{EventID: &handled.ID}, 5) {
		t.Fatal("delivery-less event did not handle")
	}

	deliveries, err := store.Backfill("agent", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 || deliveries[0].EventID != missing.ID || deliveries[1].EventID != failedEvent.ID {
		t.Fatalf("backfill = %#v", deliveries)
	}
}

func TestAuxiliaryStateAccessors(t *testing.T) {
	store := openTestStore(t)
	if value, err := store.GetSyncState("mm"); err != nil || value != nil {
		t.Fatalf("initial sync state = %v, %v", value, err)
	}
	for _, value := range []int64{10, 9, 11} {
		if err := store.SetSyncState("mm", value); err != nil {
			t.Fatal(err)
		}
	}
	if value, err := store.GetSyncState("mm"); err != nil || value == nil || *value != 11 {
		t.Fatalf("monotonic sync state = %v, %v", value, err)
	}

	if err := store.RecordPost(PostInput{PostID: "p", ChannelID: "c", Message: "before", EditAt: 1}, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPost(PostInput{PostID: "p", ChannelID: "ignored", Message: "after", EditAt: 2}, 200); err != nil {
		t.Fatal(err)
	}
	if changed, err := store.MarkPostDeleted("p", 300, 400); err != nil || !changed {
		t.Fatalf("MarkPostDeleted = %v, %v", changed, err)
	}
	post, err := store.GetPostState("p")
	if err != nil || post == nil || post.ChannelID != "c" || post.Message != "after" || post.DeleteAt != 300 || post.UpdatedAt != 400 {
		t.Fatalf("post state = %#v, %v", post, err)
	}

	if err := store.SetWatermark("account", "100", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.SetWatermark("account", "99", 2); err != nil {
		t.Fatal(err)
	}
	if watermark, err := store.GetWatermark("account"); err != nil || watermark == nil || *watermark != "99" {
		t.Fatalf("watermark = %v, %v", watermark, err)
	}
}

func TestReconcilerStatePreservesAndBumpsGeneration(t *testing.T) {
	store := openTestStore(t)
	state, err := store.PutReconcilerState(ReconcilerStateInput{
		OrgID: "org", PaneLabel: "agent", AgentKind: "omp",
	}, 100)
	if err != nil || state == nil || state.SessionGeneration != 0 {
		t.Fatalf("PutReconcilerState = %#v, %v", state, err)
	}
	generation, err := store.BumpGeneration("org", 200)
	if err != nil || generation != 1 {
		t.Fatalf("BumpGeneration = %d, %v", generation, err)
	}
	state, err = store.PutReconcilerState(ReconcilerStateInput{
		OrgID: "org", PaneLabel: "renamed", AgentKind: "omp",
	}, 300)
	if err != nil || state.SessionGeneration != 1 || state.PaneLabel != "renamed" {
		t.Fatalf("generation reset on upsert: %#v, %v", state, err)
	}
}
