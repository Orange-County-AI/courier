package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fixtureDatabase(t *testing.T, fixture string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "courier.sqlite")
	schema, err := os.ReadFile(filepath.Join("testdata", fixture))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(string(schema)); err != nil {
		raw.Close()
		t.Fatalf("apply %s: %v", fixture, err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func oldDatabase(t *testing.T, withSessionGeneration bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "courier.sqlite")
	schema, err := os.ReadFile(filepath.Join("testdata", "v0-schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(schema)
	if withSessionGeneration {
		text = strings.Replace(text, "  last_error TEXT,\n  created_at", "  last_error TEXT,\n  session_generation INTEGER,\n  created_at", 1)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(text); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
INSERT INTO events (id, connector, event_key, conversation_id, user, content, meta_json, raw_json, received_at, handled_at)
VALUES (1, 'mattermost', 'post-1', 'chan-1', 'sam', 'the old message', '{}', '{}', 1000, NULL),
       (2, 'mattermost', 'post-2', 'chan-1', 'sam', 'answered long ago', '{}', '{}', 900, 950)`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	columns := "(id, event_id, target, status, attempt_count, last_dispatched_at, created_at)"
	values := "('d-1', 1, 'agent', 'dispatched', 3, 1500, 1000), ('d-2', 2, 'agent', 'handled', 1, 900, 900)"
	if withSessionGeneration {
		columns = "(id, event_id, target, status, attempt_count, last_dispatched_at, session_generation, created_at)"
		values = "('d-1', 1, 'agent', 'dispatched', 3, 1500, 7, 1000), ('d-2', 2, 'agent', 'handled', 1, 900, 7, 900)"
	}
	if _, err := raw.Exec("INSERT INTO deliveries " + columns + " VALUES " + values); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func rawUserVersion(t *testing.T, path string) int {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var version int
	if err := raw.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func rawColumns(t *testing.T, path, table string) []string {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rows, err := raw.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return names
}

func rawSchema(t *testing.T, path string) []string {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rows, err := raw.Query(`SELECT type, name, tbl_name, sql FROM sqlite_master
WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var schema []string
	for rows.Next() {
		var objectType, name, table string
		var statement sql.NullString
		if err := rows.Scan(&objectType, &name, &table, &statement); err != nil {
			t.Fatal(err)
		}
		schema = append(schema, fmt.Sprintf("%s|%s|%s|%s", objectType, name, table, statement.String))
	}
	return schema
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestFreshDatabaseStartsAtCurrentSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "courier.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got := rawUserVersion(t, path); got != SchemaVersion {
		t.Fatalf("user_version = %d, want 1", got)
	}
	columns := rawColumns(t, path, "deliveries")
	for _, column := range []string{"session_generation", "read_at"} {
		if !containsString(columns, column) {
			t.Errorf("fresh deliveries missing %s: %v", column, columns)
		}
	}
	schema := strings.Join(rawSchema(t, path), "\n")
	for _, fragment := range []string{
		"idx_events_connector_key", "idx_events_unhandled", "idx_deliveries_one_open", "idx_deliveries_open",
		"CHECK(status IN ('pending','dispatched','replied','handled','failed'))",
		"WHERE status != 'failed'",
	} {
		if !strings.Contains(schema, fragment) {
			t.Errorf("fresh schema missing %q", fragment)
		}
	}
}

func TestCurrentDatabaseReopensRepeatedly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "courier.sqlite")
	for i := 0; i < 3; i++ {
		store, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if event, err := store.InsertEvent(EventInsert{
			Connector: "test", EventKey: fmt.Sprintf("e%d", i), ConversationID: "c", Content: "x",
		}, 1); err != nil || event == nil {
			t.Fatalf("InsertEvent = %#v, %v", event, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.CountEvents()
	if err != nil || count != 3 {
		t.Fatalf("CountEvents = %d, %v", count, err)
	}
	store.Close()
}

func TestV0DatabaseGainsColumnsKeepsRowsAndIsStamped(t *testing.T) {
	path := oldDatabase(t, true)
	if got := rawUserVersion(t, path); got != 0 {
		t.Fatalf("initial user_version = %d", got)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	count, err := store.CountEvents()
	if err != nil || count != 2 {
		t.Fatalf("CountEvents = %d, %v", count, err)
	}
	if event := getTestEvent(t, store, 1); event.Content != "the old message" || event.HandledAt != nil {
		t.Fatalf("event 1 changed: %#v", event)
	}
	if event := getTestEvent(t, store, 2); event.HandledAt == nil || *event.HandledAt != 950 {
		t.Fatalf("event 2 changed: %#v", event)
	}
	delivery := getTestDelivery(t, store, "d-1")
	if delivery.Status != DeliveryDispatched || delivery.AttemptCount != 3 || delivery.SessionGeneration == nil || *delivery.SessionGeneration != 7 || delivery.ReadAt != nil {
		t.Fatalf("old delivery changed: %#v", delivery)
	}
	if stamped, err := store.MarkRead("d-1", 4242); err != nil || !stamped {
		t.Fatalf("MarkRead = %v, %v", stamped, err)
	}
	if got := getTestDelivery(t, store, "d-1").ReadAt; got == nil || *got != 4242 {
		t.Fatalf("read_at = %v", got)
	}
	if got := rawUserVersion(t, path); got != 1 {
		t.Fatalf("migrated user_version = %d", got)
	}
}

func TestV0SecondOpenDoesNotRepeatStep(t *testing.T) {
	path := oldDatabase(t, true)
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if stamped, err := first.MarkRead("d-1", 4242); err != nil || !stamped {
		t.Fatalf("MarkRead = %v, %v", stamped, err)
	}
	first.Close()

	var logs []string
	second, err := Open(path, WithMigrationLogger(func(message string) { logs = append(logs, message) }))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if readAt := getTestDelivery(t, second, "d-1").ReadAt; readAt == nil || *readAt != 4242 {
		t.Fatalf("read_at changed on reopen: %v", readAt)
	}
	if len(logs) != 0 {
		t.Fatalf("current reopen logged migration: %v", logs)
	}
}

func TestV0PredatingSessionGenerationIsHealed(t *testing.T) {
	path := oldDatabase(t, false)
	if containsString(rawColumns(t, path, "deliveries"), "session_generation") {
		t.Fatal("fixture unexpectedly has session_generation")
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if !containsString(rawColumns(t, path, "deliveries"), "session_generation") {
		t.Fatal("migration did not add session_generation")
	}
	if got := getTestDelivery(t, store, "d-1").SessionGeneration; got != nil {
		t.Fatalf("healed session_generation = %v, want nil", got)
	}
	if err := store.ReleaseToPending("d-1", "host restarted", 1900); err != nil {
		t.Fatal(err)
	}
	generation := int64(9)
	claimed, err := store.ClaimNext("agent", 2000, &generation)
	if err != nil || claimed == nil || claimed.Delivery.ID != "d-1" {
		t.Fatalf("ClaimNext = %#v, %v", claimed, err)
	}
	if got := getTestDelivery(t, store, "d-1").SessionGeneration; got == nil || *got != 9 {
		t.Fatalf("session_generation = %v, want 9", got)
	}
}

func TestNewerDatabaseIsRefusedUntouched(t *testing.T) {
	path := fixtureDatabase(t, "current-ts-schema.sql")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 2"); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	raw.Close()
	before := rawSchema(t, path)
	store, err := Open(path)
	if store != nil {
		store.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "version 2") || !strings.Contains(err.Error(), "(1)") || !strings.Contains(err.Error(), "Refusing") {
		t.Fatalf("Open newer database error = %v", err)
	}
	if got := rawUserVersion(t, path); got != 2 {
		t.Fatalf("refusal changed user_version to %d", got)
	}
	if after := rawSchema(t, path); !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal changed schema\nbefore=%v\nafter=%v", before, after)
	}
}

func TestMigrationLogsOnlyUpgrade(t *testing.T) {
	path := oldDatabase(t, false)
	var upgrade []string
	store, err := Open(path, WithMigrationLogger(func(message string) { upgrade = append(upgrade, message) }))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	joined := strings.Join(upgrade, "\n")
	for _, fragment := range []string{
		"migrating database from version 0 to 1",
		"added deliveries.read_at",
		"healed deliveries.session_generation",
	} {
		if !strings.Contains(joined, fragment) {
			t.Errorf("migration log missing %q:\n%s", fragment, joined)
		}
	}
	var reopen []string
	store, err = Open(path, WithMigrationLogger(func(message string) { reopen = append(reopen, message) }))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if len(reopen) != 0 {
		t.Fatalf("current database logged: %v", reopen)
	}
}

func TestCurrentTypeScriptSchemaOpensWithZeroSteps(t *testing.T) {
	path := fixtureDatabase(t, "current-ts-schema.sql")
	before := rawSchema(t, path)
	var logs []string
	store, err := Open(path, WithMigrationLogger(func(message string) { logs = append(logs, message) }))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("TS current schema ran migration steps: %v", logs)
	}
	if got := rawUserVersion(t, path); got != 1 {
		t.Fatalf("user_version = %d", got)
	}
	if after := rawSchema(t, path); !reflect.DeepEqual(before, after) {
		t.Fatalf("opening TS schema changed sqlite_master\nbefore=%v\nafter=%v", before, after)
	}
}
