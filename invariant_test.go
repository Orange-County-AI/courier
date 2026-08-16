package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// No behavioral suite can notice a new writer in a path it did not imagine.
// This audit makes settlement authority structural: widening it requires a
// visible change here, where the first question is whether a human saw an answer.
type writeHit struct {
	file    string
	offset  int
	label   string
	excerpt string
}

var handledWritePatterns = []struct {
	label string
	re    *regexp.Regexp
}{
	{"SET handled_at", regexp.MustCompile(`(?i)SET\s+handled_at`)},
	{"handled_at = value", regexp.MustCompile(`handled_at\s*=\s*[^=]`)},
	{"status = handled", regexp.MustCompile(`=\s*'handled'`)},
}

func productionGoFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files
}

func commentLine(source string, offset int) bool {
	start := strings.LastIndex(source[:offset], "\n") + 1
	end := strings.Index(source[offset:], "\n")
	if end < 0 {
		end = len(source)
	} else {
		end += offset
	}
	line := strings.TrimSpace(source[start:end])
	return strings.HasPrefix(line, "//") || strings.HasPrefix(line, "/*") || strings.HasPrefix(line, "*")
}

func handledWrites(t *testing.T) []writeHit {
	t.Helper()
	var hits []writeHit
	for _, file := range productionGoFiles(t) {
		bytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		source := string(bytes)
		for _, pattern := range handledWritePatterns {
			for _, match := range pattern.re.FindAllStringIndex(source, -1) {
				if commentLine(source, match[0]) {
					continue
				}
				if pattern.label == "status = handled" && match[0] > 0 && strings.ContainsRune("!<>=", rune(source[match[0]-1])) {
					continue
				}
				start := strings.LastIndex(source[:match[0]], "\n") + 1
				end := strings.Index(source[match[0]:], "\n")
				if end < 0 {
					end = len(source)
				} else {
					end += match[0]
				}
				hits = append(hits, writeHit{file: file, offset: match[0], label: pattern.label, excerpt: strings.TrimSpace(source[start:end])})
			}
		}
	}
	return hits
}

type parsedFile struct {
	name   string
	source string
	fset   *token.FileSet
	file   *ast.File
}

func parseProductionFiles(t *testing.T) []parsedFile {
	t.Helper()
	var parsed []parsedFile
	for _, name := range productionGoFiles(t) {
		bytes, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, bytes, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed = append(parsed, parsedFile{name: name, source: string(bytes), fset: fset, file: file})
	}
	return parsed
}

func storeMethods(t *testing.T, parsed parsedFile) map[string]*ast.FuncDecl {
	t.Helper()
	methods := make(map[string]*ast.FuncDecl)
	for _, declaration := range parsed.file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || function.Body == nil {
			continue
		}
		methods[function.Name.Name] = function
	}
	return methods
}

func offsetOf(parsed parsedFile, position token.Pos) int {
	return parsed.fset.Position(position).Offset
}

func containsPosition(function *ast.FuncDecl, position token.Pos) bool {
	return function != nil && function.Body.Pos() <= position && position < function.Body.End()
}

func bodyText(parsed parsedFile, function *ast.FuncDecl) string {
	return parsed.source[offsetOf(parsed, function.Body.Pos()):offsetOf(parsed, function.Body.End())]
}

func selectorCalls(parsed parsedFile, name string) []token.Pos {
	var positions []token.Pos
	ast.Inspect(parsed.file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == name {
			positions = append(positions, call.Pos())
		}
		return true
	})
	return positions
}

func TestInvariantHandledWritersLiveOnlyInStore(t *testing.T) {
	for _, hit := range handledWrites(t) {
		if hit.file != "store.go" {
			t.Errorf("%s: %s: %s", hit.file, hit.label, hit.excerpt)
		}
	}
}

func TestInvariantStoreWritersStayInsideSanctionedMethods(t *testing.T) {
	var store parsedFile
	for _, parsed := range parseProductionFiles(t) {
		if parsed.name == "store.go" {
			store = parsed
			break
		}
	}
	methods := storeMethods(t, store)
	for _, hit := range handledWrites(t) {
		if hit.file != "store.go" {
			continue
		}
		position := store.fset.File(store.file.Pos()).Pos(hit.offset)
		if !containsPosition(methods["handle"], position) && !containsPosition(methods["MarkHandled"], position) {
			t.Errorf("store.go writer outside handle/MarkHandled: %s", hit.excerpt)
		}
	}
}

func TestInvariantHandleHasOnlyTwoCallerMethods(t *testing.T) {
	allowed := map[string]bool{"CompleteAfterPost": true, "MarkHandled": true}
	callers := make(map[string]bool)
	for _, parsed := range parseProductionFiles(t) {
		methods := storeMethods(t, parsed)
		for _, position := range selectorCalls(parsed, "handle") {
			caller := ""
			for name, method := range methods {
				if containsPosition(method, position) {
					caller = name
					break
				}
			}
			if parsed.name != "store.go" || !allowed[caller] {
				t.Errorf("%s has Store.handle call in %s", parsed.name, caller)
			}
			callers[caller] = true
		}
	}
	if !callers["CompleteAfterPost"] || !callers["MarkHandled"] || len(callers) != 2 {
		t.Fatalf("handle callers = %v, want CompleteAfterPost and MarkHandled", callers)
	}
}

func TestInvariantCompleteAfterPostNilGuardPrecedesHandle(t *testing.T) {
	parsed := parseProductionFiles(t)
	var store parsedFile
	for _, file := range parsed {
		if file.name == "store.go" {
			store = file
		}
	}
	method := storeMethods(t, store)["CompleteAfterPost"]
	calls := selectorCalls(store, "handle")
	var handlePosition token.Pos
	for _, position := range calls {
		if containsPosition(method, position) {
			handlePosition = position
			break
		}
	}
	var guardPosition token.Pos
	ast.Inspect(method.Body, func(node ast.Node) bool {
		statement, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		condition := store.source[offsetOf(store, statement.Cond.Pos()):offsetOf(store, statement.Cond.End())]
		if !strings.Contains(condition, "PostedAt") || !strings.Contains(condition, "nil") {
			return true
		}
		for _, bodyStatement := range statement.Body.List {
			if _, ok := bodyStatement.(*ast.ReturnStmt); ok {
				guardPosition = statement.Pos()
			}
		}
		return true
	})
	if guardPosition == token.NoPos || handlePosition == token.NoPos || guardPosition >= handlePosition {
		t.Fatalf("posted_at nil guard %v must return before handle call %v", guardPosition, handlePosition)
	}
}

func TestInvariantConversationHandlingRoutesThroughMarkHandled(t *testing.T) {
	var store parsedFile
	for _, parsed := range parseProductionFiles(t) {
		if parsed.name == "store.go" {
			store = parsed
		}
	}
	method := storeMethods(t, store)["MarkConversationHandled"]
	body := bodyText(store, method)
	if !strings.Contains(body, ".MarkHandled(") {
		t.Fatal("MarkConversationHandled does not route through MarkHandled")
	}
	if regexp.MustCompile(`(?i)UPDATE\s+(events|deliveries)`).MatchString(body) || strings.Contains(body, ".handle(") {
		t.Fatalf("MarkConversationHandled became a writer/private caller:\n%s", body)
	}
}

func TestInvariantMarkReadWritesOnlyStampOnce(t *testing.T) {
	var store parsedFile
	for _, parsed := range parseProductionFiles(t) {
		if parsed.name == "store.go" {
			store = parsed
		}
	}
	method := storeMethods(t, store)["MarkRead"]
	body := bodyText(store, method)
	if !strings.Contains(body, "UPDATE deliveries SET read_at") || !strings.Contains(body, "read_at IS NULL") {
		t.Fatalf("MarkRead lost stamp-once update:\n%s", body)
	}
	for _, forbidden := range []string{"handled_at", "'handled'", ".handle(", "status"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("MarkRead contains forbidden %q:\n%s", forbidden, body)
		}
	}

	writer := regexp.MustCompile(`read_at\s*=\s*[^=]`)
	for _, parsed := range parseProductionFiles(t) {
		for _, match := range writer.FindAllStringIndex(parsed.source, -1) {
			if commentLine(parsed.source, match[0]) {
				continue
			}
			position := parsed.fset.File(parsed.file.Pos()).Pos(match[0])
			if parsed.name != "store.go" || !containsPosition(method, position) {
				t.Errorf("read_at writer outside MarkRead: %s at %d", parsed.name, match[0])
			}
		}
	}
}

func TestInvariantMarkReadCallersAreOnlyHostTools(t *testing.T) {
	var callers []string
	for _, parsed := range parseProductionFiles(t) {
		if parsed.name == "store.go" {
			continue
		}
		if len(selectorCalls(parsed, "MarkRead")) > 0 {
			callers = append(callers, filepath.Base(parsed.name))
		}
	}
	// Integration tightens this naturally when hosttools.go lands: until then
	// the allowed set is a subset, never an alternate caller.
	for _, caller := range callers {
		if caller != "hosttools.go" {
			t.Errorf("MarkRead caller = %s, want only hosttools.go", caller)
		}
	}
	if len(callers) > 1 {
		t.Fatalf("MarkRead callers = %v", callers)
	}
}

func invariantFixture(t *testing.T) (*Store, *Event, *Delivery) {
	t.Helper()
	store := openTestStore(t,
		WithRedeliverGrace(100),
		WithRedeliverReadFactor(4),
		WithRedeliverMaxBackoff(800),
	)
	event := insertTestEvent(t, store, "event", "conv", 1000)
	delivery := insertTestDelivery(t, store, event.ID, "agent", 1000)
	return store, event, delivery
}

func assertUnhandled(t *testing.T, store *Store, eventID int64) {
	t.Helper()
	if handledAt := getTestEvent(t, store, eventID).HandledAt; handledAt != nil {
		t.Fatalf("event unexpectedly handled at %d", *handledAt)
	}
}

func TestInvariantSuccessfulPromptDoesNotHandle(t *testing.T) {
	store, event, _ := invariantFixture(t)
	claimed, err := store.ClaimNext("agent", 1100, nil)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext = %#v, %v", claimed, err)
	}
	if err := store.ConfirmDispatched(claimed.Delivery.ID, 1200); err != nil {
		t.Fatal(err)
	}
	assertUnhandled(t, store, event.ID)
}

func TestInvariantBlockedSettleDoesNotHandle(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	if _, err := store.ClaimNext("agent", 1100, nil); err != nil {
		t.Fatal(err)
	}
	if getTestDelivery(t, store, delivery.ID).Status != DeliveryDispatched {
		t.Fatal("blocked store half did not stay dispatched")
	}
	assertUnhandled(t, store, event.ID)
}

func TestInvariantFailureSweepAndReconcileStoreHalvesDoNotHandle(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	if _, err := store.ClaimNext("agent", 1100, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseToPending(delivery.ID, "timeout", 1200); err != nil {
		t.Fatal(err)
	}
	if reclaimed, err := store.SweepStuckDispatches(10_000); err != nil || len(reclaimed) != 0 {
		t.Fatalf("SweepStuckDispatches = %v, %v", reclaimed, err)
	}
	if reclaimed, err := store.ReclaimStaleDispatches(100, 10_000); err != nil || len(reclaimed) != 0 {
		t.Fatalf("ReclaimStaleDispatches = %v, %v", reclaimed, err)
	}
	assertUnhandled(t, store, event.ID)
}

func TestInvariantFailedPostDoesNotHandle(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	if _, err := store.ClaimNext("agent", 1100, nil); err != nil {
		t.Fatal(err)
	}
	reply, duplicate, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "answer",
	}, 1200)
	if err != nil || duplicate {
		t.Fatalf("InsertReply = %#v, %v, %v", reply, duplicate, err)
	}
	if err := store.MarkPostError(reply.ID, "connector down"); err != nil {
		t.Fatal(err)
	}
	assertUnhandled(t, store, event.ID)
	if status := getTestDelivery(t, store, delivery.ID).Status; status != DeliveryReplied {
		t.Fatalf("delivery status = %s, want replied", status)
	}
}

func TestInvariantCompleteAfterPostRefusesUnpostedReply(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	reply, _, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "answer",
	}, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if store.CompleteAfterPost(reply.ID, 1300) {
		t.Fatal("CompleteAfterPost accepted nil posted_at")
	}
	assertUnhandled(t, store, event.ID)
	if err := store.MarkPosted(reply.ID, 1400); err != nil {
		t.Fatal(err)
	}
	if !store.CompleteAfterPost(reply.ID, 1500) {
		t.Fatal("CompleteAfterPost refused confirmed post")
	}
}

func TestInvariantCompleteAfterPostAcceptsAutoSettledDelivery(t *testing.T) {
	store, _, delivery := invariantFixture(t)
	reply, _, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "answer",
	}, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := store.MarkConversationHandled("mattermost", "conv", 1300, 1300); err != nil || changed != 1 {
		t.Fatalf("MarkConversationHandled = %d, %v", changed, err)
	}
	if err := store.MarkPosted(reply.ID, 1400); err != nil {
		t.Fatal(err)
	}
	if !store.CompleteAfterPost(reply.ID, 1500) {
		t.Fatal("CompleteAfterPost rejected a confirmed post whose connector already settled the delivery")
	}
}

func TestInvariantRefusedReplyStoreHalfCannotSettle(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	// Target/conversation 409s live in hosttools. Their store-level half is that
	// validation performs no write and even a bogus completion id is a no-op.
	if store.CompleteAfterPost("not-a-reply", 1200) {
		t.Fatal("missing reply completed")
	}
	assertUnhandled(t, store, event.ID)
	if getTestDelivery(t, store, delivery.ID).Status != DeliveryPending {
		t.Fatal("refused reply changed delivery")
	}
}

func TestInvariantMarkConversationHandledScopesInclusiveBoundary(t *testing.T) {
	store := openTestStore(t)
	now := int64(1000)
	rows := []struct {
		key          string
		conversation string
		receivedAt   int64
	}{
		{"a", "conv-1", now - 10},
		{"b", "conv-1", now},
		{"c", "conv-1", now + 10},
		{"d", "conv-2", now - 10},
	}
	ids := make(map[string]int64)
	for _, row := range rows {
		event := insertTestEvent(t, store, row.key, row.conversation, row.receivedAt)
		ids[row.key] = event.ID
	}
	other, err := store.InsertEvent(EventInsert{
		Connector: "gmail", EventKey: "e", ConversationID: "conv-1", Content: "e",
	}, now-10)
	if err != nil || other == nil {
		t.Fatalf("InsertEvent foreign = %#v, %v", other, err)
	}
	changed, err := store.MarkConversationHandled("mattermost", "conv-1", now, now)
	if err != nil || changed != 2 {
		t.Fatalf("MarkConversationHandled = %d, %v", changed, err)
	}
	for _, key := range []string{"a", "b"} {
		if getTestEvent(t, store, ids[key]).HandledAt == nil {
			t.Errorf("included event %s remains unhandled", key)
		}
	}
	for _, key := range []string{"c", "d"} {
		assertUnhandled(t, store, ids[key])
	}
	assertUnhandled(t, store, other.ID)
	changed, err = store.MarkConversationHandled("mattermost", "conv-1", now, now)
	if err != nil || changed != 0 {
		t.Fatalf("idempotent MarkConversationHandled = %d, %v", changed, err)
	}
}

func TestInvariantReadThenCrashStillRedelivers(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	if _, err := store.ClaimNext("agent", 1100, nil); err != nil {
		t.Fatal(err)
	}
	if stamped, err := store.MarkRead(delivery.ID, 1200); err != nil || !stamped {
		t.Fatalf("MarkRead = %v, %v", stamped, err)
	}
	if stamped, err := store.MarkRead(delivery.ID, 1300); err != nil || stamped {
		t.Fatalf("second MarkRead = %v, %v", stamped, err)
	}
	if reclaimed, err := store.SweepStuckDispatches(1900); err != nil || len(reclaimed) != 1 || reclaimed[0] != delivery.ID {
		t.Fatalf("read delivery did not return: %v, %v", reclaimed, err)
	}
	if claimed, err := store.ClaimNext("agent", 2000, nil); err != nil || claimed == nil {
		t.Fatalf("redelivery claim = %#v, %v", claimed, err)
	}
	assertUnhandled(t, store, event.ID)
}

func TestInvariantConfirmedPostHandles(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	reply, _, err := store.InsertReply(ReplyInsert{
		DeliveryID: delivery.ID, Target: "agent", ConversationID: "conv", Message: "answered",
	}, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPosted(reply.ID, 1300); err != nil {
		t.Fatal(err)
	}
	if !store.CompleteAfterPost(reply.ID, 1400) {
		t.Fatal("confirmed post did not handle")
	}
	if getTestEvent(t, store, event.ID).HandledAt == nil || getTestDelivery(t, store, delivery.ID).Status != DeliveryHandled {
		t.Fatal("confirmed post did not settle both rows")
	}
}

func TestInvariantExplicitMarkHandledHandles(t *testing.T) {
	store, event, delivery := invariantFixture(t)
	if !store.MarkHandled(MarkHandledArgs{DeliveryID: delivery.ID}, 1400) {
		t.Fatal("explicit MarkHandled did not handle")
	}
	if getTestEvent(t, store, event.ID).HandledAt == nil || getTestDelivery(t, store, delivery.ID).Status != DeliveryHandled {
		t.Fatal("explicit MarkHandled did not settle both rows")
	}
}
