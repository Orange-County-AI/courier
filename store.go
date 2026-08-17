package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

const (
	SchemaVersion                = 2
	DefaultRedeliverGraceMS      = int64(300_000)
	DefaultRedeliverMaxBackoffMS = int64(1_800_000)
	DefaultRedeliverReadFactor   = int64(4)
)

type DeliveryStatus string

const (
	DeliveryPending    DeliveryStatus = "pending"
	DeliveryDispatched DeliveryStatus = "dispatched"
	DeliveryReplied    DeliveryStatus = "replied"
	DeliveryHandled    DeliveryStatus = "handled"
	DeliveryFailed     DeliveryStatus = "failed"
)

type Event struct {
	ID             int64
	Connector      string
	EventKey       string
	ConversationID string
	User           *string
	Content        string
	MetaJSON       string
	RawJSON        string
	ReceivedAt     int64
	HandledAt      *int64
}

type EventInsert struct {
	Connector      string
	EventKey       string
	ConversationID string
	User           *string
	Content        string
	MetaJSON       string
	RawJSON        string
}

type Delivery struct {
	ID                string
	EventID           int64
	Target            string
	Status            DeliveryStatus
	AttemptCount      int64
	LastDispatchedAt  *int64
	LastError         *string
	SessionGeneration *int64
	ReadAt            *int64
	CreatedAt         int64
}

type Reply struct {
	ID             string
	DeliveryID     string
	Target         string
	ConversationID string
	Message        string
	CreatedAt      int64
	PostedAt       *int64
	PostError      *string
}

type ReplyInsert struct {
	DeliveryID     string
	Target         string
	ConversationID string
	Message        string
}

type Deliverable struct {
	Delivery Delivery
	Event    Event
}

type ReconcilerState struct {
	OrgID               string
	HerdrSession        *string
	WorkspaceID         *string
	PaneID              *string
	PaneLabel           string
	AgentKind           string
	NativeSessionSource *string
	NativeSessionKind   *string
	NativeSessionValue  *string
	SessionGeneration   int64
	UpdatedAt           int64
}

type ReconcilerStateInput struct {
	OrgID               string
	HerdrSession        *string
	WorkspaceID         *string
	PaneID              *string
	PaneLabel           string
	AgentKind           string
	NativeSessionSource *string
	NativeSessionKind   *string
	NativeSessionValue  *string
	SessionGeneration   *int64
}

type PostState struct {
	PostID    string
	ChannelID string
	Message   string
	EditAt    int64
	DeleteAt  int64
	UpdatedAt int64
}

type PostInput struct {
	PostID    string
	ChannelID string
	Message   string
	EditAt    int64
}

type DeliveryStats struct {
	Unread           int64  `json:"unread"`
	OldestUnreadAgeS *int64 `json:"oldest_unread_age_s"`
	ReadUnconfirmed  int64  `json:"read_unconfirmed"`
}

type storeOptions struct {
	redeliverGraceMS      int64
	redeliverMaxBackoffMS int64
	redeliverReadFactor   int64
	log                   func(string)
}

type StoreOption func(*storeOptions)

func WithRedeliverGrace(ms int64) StoreOption {
	return func(o *storeOptions) { o.redeliverGraceMS = ms }
}

func WithRedeliverMaxBackoff(ms int64) StoreOption {
	return func(o *storeOptions) { o.redeliverMaxBackoffMS = ms }
}

func WithRedeliverReadFactor(factor int64) StoreOption {
	return func(o *storeOptions) { o.redeliverReadFactor = factor }
}

func WithMigrationLogger(log func(string)) StoreOption {
	return func(o *storeOptions) { o.log = log }
}

type Store struct {
	db                    *sql.DB
	redeliverGraceMS      int64
	redeliverMaxBackoffMS int64
	redeliverReadFactor   int64
	log                   func(string)
}

func Open(path string, options ...StoreOption) (*Store, error) {
	opts := storeOptions{
		redeliverGraceMS:      DefaultRedeliverGraceMS,
		redeliverMaxBackoffMS: DefaultRedeliverMaxBackoffMS,
		redeliverReadFactor:   DefaultRedeliverReadFactor,
		log:                   func(string) {},
	}
	for _, option := range options {
		option(&opts)
	}
	if opts.redeliverGraceMS <= 0 || opts.redeliverMaxBackoffMS <= 0 || opts.redeliverReadFactor <= 0 {
		return nil, errors.New("redelivery grace, cap, and read factor must be positive")
	}
	if opts.log == nil {
		opts.log = func(string) {}
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One connection serializes every statement. Besides avoiding lock churn,
	// this reproduces node:sqlite's synchronous single-writer semantics; claim
	// and uniqueness behavior depend on it under concurrent dispatcher calls.
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}

	s := &Store{
		db:                    db,
		redeliverGraceMS:      opts.redeliverGraceMS,
		redeliverMaxBackoffMS: opts.redeliverMaxBackoffMS,
		redeliverReadFactor:   opts.redeliverReadFactor,
		log:                   opts.log,
	}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) userVersion() (int, error) {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}

func columns(q interface {
	Query(string, ...any) (*sql.Rows, error)
}, table string) (map[string]bool, error) {
	rows, err := q.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

func (s *Store) migrate() error {
	found, err := s.userVersion()
	if err != nil {
		return err
	}
	if found > SchemaVersion {
		return fmt.Errorf("database schema version %d is NEWER than this build understands (%d). Refusing to open it: a newer schema may carry columns and invariants this code would ignore. Run the newer build, or restore a matching database file", found, SchemaVersion)
	}
	if err := s.baseline(); err != nil {
		return err
	}
	if found == SchemaVersion {
		return nil
	}

	s.log(fmt.Sprintf("schema: migrating database from version %d to %d", found, SchemaVersion))
	for version := found + 1; version <= SchemaVersion; version++ {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin schema step %d: %w", version, err)
		}
		if err := s.step(tx, version); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
			tx.Rollback()
			return fmt.Errorf("stamp schema version %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema step %d: %w", version, err)
		}
		s.log(fmt.Sprintf("schema: at version %d", version))
	}
	return nil
}

func (s *Store) step(tx *sql.Tx, version int) error {
	switch version {
	case 1:
		cols, err := columns(tx, "deliveries")
		if err != nil {
			return fmt.Errorf("inspect deliveries columns: %w", err)
		}
		if !cols["read_at"] {
			if _, err := tx.Exec("ALTER TABLE deliveries ADD COLUMN read_at INTEGER"); err != nil {
				return fmt.Errorf("add deliveries.read_at: %w", err)
			}
			s.log("schema: added deliveries.read_at")
		}
		// session_generation originally arrived by editing CREATE TABLE. Healing old
		// files here is why versioned migration exists: baseline edits reach only new files.
		if !cols["session_generation"] {
			if _, err := tx.Exec("ALTER TABLE deliveries ADD COLUMN session_generation INTEGER"); err != nil {
				return fmt.Errorf("add deliveries.session_generation: %w", err)
			}
			s.log("schema: healed deliveries.session_generation (predates the CREATE-edit that added it)")
		}
		return nil
	case 2:
		if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS mattermost_threads (
          channel_id TEXT NOT NULL,
          root_id TEXT NOT NULL,
          followed_at INTEGER NOT NULL,
          PRIMARY KEY(channel_id, root_id)
        )`); err != nil {
			return fmt.Errorf("create mattermost_threads: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("no migration step defined for schema version %d", version)
	}
}

var baselineStatements = []string{
	`CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connector TEXT NOT NULL,
        event_key TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        user TEXT,
        content TEXT NOT NULL,
        meta_json TEXT NOT NULL,
        raw_json TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        handled_at INTEGER
      )`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_events_connector_key
      ON events(connector, event_key)`,
	`CREATE INDEX IF NOT EXISTS idx_events_unhandled
      ON events(handled_at) WHERE handled_at IS NULL`,
	`CREATE TABLE IF NOT EXISTS deliveries (
        id TEXT PRIMARY KEY,
        event_id INTEGER NOT NULL REFERENCES events(id),
        target TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending','dispatched','replied','handled','failed')),
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_dispatched_at INTEGER,
        last_error TEXT,
        session_generation INTEGER,
        read_at INTEGER,
        created_at INTEGER NOT NULL
      )`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_one_open
      ON deliveries(event_id) WHERE status != 'failed'`,
	`CREATE INDEX IF NOT EXISTS idx_deliveries_open
      ON deliveries(target, status)
      WHERE status IN ('pending','dispatched','replied')`,
	`CREATE TABLE IF NOT EXISTS replies (
        id TEXT PRIMARY KEY,
        delivery_id TEXT NOT NULL REFERENCES deliveries(id),
        target TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        posted_at INTEGER,
        post_error TEXT,
        UNIQUE(delivery_id, target)
      )`,
	`CREATE TABLE IF NOT EXISTS reconciler_state (
        org_id TEXT PRIMARY KEY,
        herdr_session TEXT,
        workspace_id TEXT,
        pane_id TEXT,
        pane_label TEXT NOT NULL,
        agent_kind TEXT NOT NULL,
        native_session_source TEXT,
        native_session_kind TEXT,
        native_session_value TEXT,
        session_generation INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL
      )`,
	`CREATE TABLE IF NOT EXISTS sync_state (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      )`,
	`CREATE TABLE IF NOT EXISTS post_state (
        post_id TEXT PRIMARY KEY,
        channel_id TEXT NOT NULL,
        message TEXT NOT NULL,
        edit_at INTEGER NOT NULL DEFAULT 0,
        delete_at INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL
      )`,
	`CREATE TABLE IF NOT EXISTS mattermost_threads (
        channel_id TEXT NOT NULL,
        root_id TEXT NOT NULL,
        followed_at INTEGER NOT NULL,
        PRIMARY KEY(channel_id, root_id)
      )`,
	`CREATE TABLE IF NOT EXISTS watermarks (
        account TEXT PRIMARY KEY,
        history_id TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )`,
}

func (s *Store) baseline() error {
	for _, statement := range baselineStatements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("create current schema: %w", err)
		}
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func nullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Int64
	return &v
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

// eventColumns spells out the order scanEvent expects. Queries that read events
// alone can rely on `SELECT *`; a join cannot, because it has to interleave two
// tables' columns.
const eventColumns = "id, connector, event_key, conversation_id, user, content, meta_json, raw_json, received_at, handled_at"

func prefixColumns(alias, columns string) string {
	parts := strings.Split(columns, ", ")
	for i, part := range parts {
		parts[i] = alias + "." + part
	}
	return strings.Join(parts, ", ")
}

func deliveryColumnsPrefixed(alias string) string { return prefixColumns(alias, deliveryColumns) }

func eventColumnsPrefixed(alias string) string { return prefixColumns(alias, eventColumns) }

// scanDeliverableRow reads deliveryColumns followed by eventColumns from one
// joined row. The two field lists are spelled out once each here because a
// single Scan cannot be composed from scanDelivery and scanEvent.
func scanDeliverableRow(row rowScanner, delivery *Delivery, event *Event) error {
	var status string
	var lastDispatchedAt, sessionGeneration, readAt, handledAt sql.NullInt64
	var lastError, user sql.NullString
	if err := row.Scan(
		&delivery.ID, &delivery.EventID, &delivery.Target, &status, &delivery.AttemptCount,
		&lastDispatchedAt, &lastError, &sessionGeneration, &readAt, &delivery.CreatedAt,
		&event.ID, &event.Connector, &event.EventKey, &event.ConversationID, &user,
		&event.Content, &event.MetaJSON, &event.RawJSON, &event.ReceivedAt, &handledAt,
	); err != nil {
		return err
	}
	delivery.Status = DeliveryStatus(status)
	delivery.LastDispatchedAt = nullableInt(lastDispatchedAt)
	delivery.LastError = nullableString(lastError)
	delivery.SessionGeneration = nullableInt(sessionGeneration)
	delivery.ReadAt = nullableInt(readAt)
	event.User = nullableString(user)
	event.HandledAt = nullableInt(handledAt)
	return nil
}

func scanEvent(row rowScanner) (*Event, error) {
	var event Event
	var user sql.NullString
	var handledAt sql.NullInt64
	if err := row.Scan(&event.ID, &event.Connector, &event.EventKey, &event.ConversationID, &user,
		&event.Content, &event.MetaJSON, &event.RawJSON, &event.ReceivedAt, &handledAt); err != nil {
		return nil, err
	}
	event.User = nullableString(user)
	event.HandledAt = nullableInt(handledAt)
	return &event, nil
}

const deliveryColumns = "id, event_id, target, status, attempt_count, last_dispatched_at, last_error, session_generation, read_at, created_at"

func scanDelivery(row rowScanner) (*Delivery, error) {
	var delivery Delivery
	var status string
	var lastDispatchedAt, sessionGeneration, readAt sql.NullInt64
	var lastError sql.NullString
	if err := row.Scan(&delivery.ID, &delivery.EventID, &delivery.Target, &status, &delivery.AttemptCount,
		&lastDispatchedAt, &lastError, &sessionGeneration, &readAt, &delivery.CreatedAt); err != nil {
		return nil, err
	}
	delivery.Status = DeliveryStatus(status)
	delivery.LastDispatchedAt = nullableInt(lastDispatchedAt)
	delivery.LastError = nullableString(lastError)
	delivery.SessionGeneration = nullableInt(sessionGeneration)
	delivery.ReadAt = nullableInt(readAt)
	return &delivery, nil
}

func scanReply(row rowScanner) (*Reply, error) {
	var reply Reply
	var postedAt sql.NullInt64
	var postError sql.NullString
	if err := row.Scan(&reply.ID, &reply.DeliveryID, &reply.Target, &reply.ConversationID, &reply.Message,
		&reply.CreatedAt, &postedAt, &postError); err != nil {
		return nil, err
	}
	reply.PostedAt = nullableInt(postedAt)
	reply.PostError = nullableString(postError)
	return &reply, nil
}

func noRows[T any](value *T, err error) (*T, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return value, err
}

func (s *Store) InsertEvent(event EventInsert, now int64) (*Event, error) {
	metaJSON := event.MetaJSON
	if metaJSON == "" {
		metaJSON = "{}"
	}
	rawJSON := event.RawJSON
	if rawJSON == "" {
		rawJSON = "{}"
	}
	row := s.db.QueryRow(`
        INSERT INTO events (connector, event_key, conversation_id, user, content,
                            meta_json, raw_json, received_at, handled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(connector, event_key) DO NOTHING
        RETURNING *
      `, event.Connector, event.EventKey, event.ConversationID, event.User, event.Content, metaJSON, rawJSON, now)
	inserted, err := scanEvent(row)
	return noRows(inserted, err)
}

func (s *Store) GetEvent(id int64) (*Event, error) {
	event, err := scanEvent(s.db.QueryRow("SELECT * FROM events WHERE id = ?", id))
	return noRows(event, err)
}

func (s *Store) FindEvent(connector, eventKey string) (*Event, error) {
	event, err := scanEvent(s.db.QueryRow("SELECT * FROM events WHERE connector = ? AND event_key = ?", connector, eventKey))
	return noRows(event, err)
}

func (s *Store) CountEvents(connector ...string) (int64, error) {
	var count int64
	var err error
	if len(connector) == 0 {
		err = s.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
	} else {
		err = s.db.QueryRow("SELECT COUNT(*) FROM events WHERE connector = ?", connector[0]).Scan(&count)
	}
	return count, err
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	var encoded [36]byte
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])
	return string(encoded[:]), nil
}

func (s *Store) InsertDelivery(eventID int64, target string, now int64) (*Delivery, error) {
	// The transaction restores node:sqlite's method-level serialization. Without
	// it, two goroutines could both pass the idempotency read before either insert.
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	existing, err := scanDelivery(tx.QueryRow("SELECT "+deliveryColumns+" FROM deliveries WHERE event_id = ? AND status != 'failed'", eventID))
	if err == nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`
        INSERT INTO deliveries (id, event_id, target, status, attempt_count,
                                last_dispatched_at, last_error, session_generation, created_at)
        VALUES (?, ?, ?, 'pending', 0, NULL, NULL, NULL, ?)
      `, id, eventID, target, now); err != nil {
		return nil, err
	}
	delivery, err := scanDelivery(tx.QueryRow("SELECT "+deliveryColumns+" FROM deliveries WHERE id = ?", id))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return delivery, nil
}

func (s *Store) GetDelivery(id string) (*Delivery, error) {
	delivery, err := scanDelivery(s.db.QueryRow("SELECT "+deliveryColumns+" FROM deliveries WHERE id = ?", id))
	return noRows(delivery, err)
}

func (s *Store) OpenDeliveryForEvent(eventID int64) (*Delivery, error) {
	delivery, err := scanDelivery(s.db.QueryRow("SELECT "+deliveryColumns+" FROM deliveries WHERE event_id = ? AND status != 'failed'", eventID))
	return noRows(delivery, err)
}

func (s *Store) DeliveriesForTarget(target string) ([]Delivery, error) {
	rows, err := s.db.Query("SELECT "+deliveryColumns+" FROM deliveries WHERE target = ? ORDER BY created_at ASC, id ASC", target)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deliveries []Delivery
	for rows.Next() {
		delivery, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, *delivery)
	}
	return deliveries, rows.Err()
}

// OpenDeliveries is the queue a human can still act on: pending and dispatched
// rows for one target whose event is unhandled, oldest first by event id — the
// same ordering ClaimNext dispatches in, so the inbox lists what is next. The
// join keeps it one query; handled, failed and other targets' rows are absent.
func (s *Store) OpenDeliveries(target string) ([]Deliverable, error) {
	rows, err := s.db.Query(`
        SELECT `+deliveryColumnsPrefixed("d")+`, `+eventColumnsPrefixed("e")+`
        FROM deliveries d
        JOIN events e ON e.id = d.event_id
        WHERE d.target = ? AND d.status IN ('pending','dispatched') AND e.handled_at IS NULL
        ORDER BY e.id ASC`, target)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	open := make([]Deliverable, 0)
	for rows.Next() {
		var delivery Delivery
		var event Event
		if err := scanDeliverableRow(rows, &delivery, &event); err != nil {
			return nil, err
		}
		open = append(open, Deliverable{Delivery: delivery, Event: event})
	}
	return open, rows.Err()
}

// HasClaimable answers the exact selection ClaimNext would make, without
// claiming it. The draft guard needs to know there is work before it pays for a
// pane read, and must not burn an attempt to find out.
func (s *Store) HasClaimable(target string) (bool, error) {
	var present int
	err := s.db.QueryRow(`
        SELECT 1 FROM deliveries d
        JOIN events e ON e.id = d.event_id
        WHERE d.target = ? AND d.status = 'pending' AND e.handled_at IS NULL
        LIMIT 1`, target).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ClaimNext(target string, now int64, sessionGeneration *int64) (*Deliverable, error) {
	query := `
        UPDATE deliveries SET
          status = 'dispatched',
          attempt_count = attempt_count + 1,
          last_dispatched_at = ?,
          session_generation = ?
        WHERE id = (
          SELECT d.id FROM deliveries d
          JOIN events e ON e.id = d.event_id
          WHERE d.target = ? AND d.status = 'pending' AND e.handled_at IS NULL
          ORDER BY e.id ASC
          LIMIT 1
        )
        RETURNING ` + deliveryColumns
	delivery, err := scanDelivery(s.db.QueryRow(query, now, sessionGeneration, target))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	event, err := s.GetEvent(delivery.EventID)
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, nil
	}
	return &Deliverable{Delivery: *delivery, Event: *event}, nil
}

func (s *Store) ConfirmDispatched(deliveryID string, now int64) error {
	_, err := s.db.Exec(`UPDATE deliveries SET last_dispatched_at = ?, last_error = NULL
        WHERE id = ? AND status = 'dispatched'`, now, deliveryID)
	return err
}

func truncateError(message string) string {
	count := 0
	for index := range message {
		if count == 2000 {
			return message[:index]
		}
		count++
	}
	return message
}

func (s *Store) ReleaseToPending(deliveryID, message string, now int64) error {
	_, err := s.db.Exec(`UPDATE deliveries SET status = 'pending', last_error = ?, last_dispatched_at = ?
        WHERE id = ? AND status = 'dispatched'`, truncateError(message), now, deliveryID)
	return err
}

func (s *Store) FailDelivery(deliveryID, message string) error {
	_, err := s.db.Exec("UPDATE deliveries SET status = 'failed', last_error = ? WHERE id = ?", truncateError(message), deliveryID)
	return err
}

func (s *Store) MarkRead(deliveryID string, now int64) (bool, error) {
	result, err := s.db.Exec("UPDATE deliveries SET read_at = ? WHERE id = ? AND read_at IS NULL", now, deliveryID)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed > 0, err
}

func (s *Store) DeliveryStats(target string, now int64) (DeliveryStats, error) {
	rows, err := s.db.Query("SELECT read_at, created_at FROM deliveries WHERE target = ? AND status IN ('pending','dispatched')", target)
	if err != nil {
		return DeliveryStats{}, err
	}
	defer rows.Close()
	var stats DeliveryStats
	var oldest *int64
	for rows.Next() {
		var readAt sql.NullInt64
		var createdAt int64
		if err := rows.Scan(&readAt, &createdAt); err != nil {
			return DeliveryStats{}, err
		}
		if readAt.Valid {
			stats.ReadUnconfirmed++
			continue
		}
		stats.Unread++
		if oldest == nil || createdAt < *oldest {
			v := createdAt
			oldest = &v
		}
	}
	if err := rows.Err(); err != nil {
		return DeliveryStats{}, err
	}
	if oldest != nil {
		age := now - *oldest
		if age < 0 {
			age = 0
		} else {
			age = (age + 500) / 1000
		}
		stats.OldestUnreadAgeS = &age
	}
	return stats, nil
}

func (s *Store) BackoffMS(attemptCount int64, read bool) int64 {
	exponent := attemptCount - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 30 {
		exponent = 30
	}
	base := s.redeliverGraceMS
	if read {
		if s.redeliverReadFactor > s.redeliverMaxBackoffMS/base {
			return s.redeliverMaxBackoffMS
		}
		base *= s.redeliverReadFactor
	}
	if base >= s.redeliverMaxBackoffMS {
		return s.redeliverMaxBackoffMS
	}
	factor := int64(1) << exponent
	if factor > s.redeliverMaxBackoffMS/base {
		return s.redeliverMaxBackoffMS
	}
	return base * factor
}

func (s *Store) SweepStuckDispatches(now int64, target ...string) ([]string, error) {
	query := "SELECT " + deliveryColumns + " FROM deliveries WHERE status = 'dispatched'"
	var args []any
	if len(target) > 0 {
		query += " AND target = ?"
		args = append(args, target[0])
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	var due []string
	for rows.Next() {
		delivery, err := scanDelivery(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if delivery.LastDispatchedAt == nil || now-*delivery.LastDispatchedAt >= s.BackoffMS(delivery.AttemptCount, delivery.ReadAt != nil) {
			due = append(due, delivery.ID)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var reclaimed []string
	for _, id := range due {
		result, err := s.db.Exec("UPDATE deliveries SET status = 'pending' WHERE id = ? AND status = 'dispatched'", id)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if changed > 0 {
			reclaimed = append(reclaimed, id)
		}
	}
	return reclaimed, nil
}

func (s *Store) ReclaimStaleDispatches(olderThanMS, now int64) ([]string, error) {
	rows, err := s.db.Query("SELECT " + deliveryColumns + " FROM deliveries WHERE status = 'dispatched'")
	if err != nil {
		return nil, err
	}
	var stale []string
	for rows.Next() {
		delivery, err := scanDelivery(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if delivery.LastDispatchedAt == nil || now-*delivery.LastDispatchedAt >= olderThanMS {
			stale = append(stale, delivery.ID)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var reclaimed []string
	for _, id := range stale {
		result, err := s.db.Exec("UPDATE deliveries SET status = 'pending' WHERE id = ? AND status = 'dispatched'", id)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if changed > 0 {
			reclaimed = append(reclaimed, id)
		}
	}
	return reclaimed, nil
}

func (s *Store) InsertReply(reply ReplyInsert, now int64) (*Reply, bool, error) {
	id, err := newID()
	if err != nil {
		return nil, false, err
	}
	inserted, err := scanReply(s.db.QueryRow(`
        INSERT INTO replies (id, delivery_id, target, conversation_id, message, created_at, posted_at, post_error)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)
        ON CONFLICT(delivery_id, target) DO NOTHING
        RETURNING *
      `, id, reply.DeliveryID, reply.Target, reply.ConversationID, reply.Message, now))
	if err == nil {
		if _, err := s.db.Exec("UPDATE deliveries SET status = 'replied' WHERE id = ? AND status IN ('pending','dispatched')", reply.DeliveryID); err != nil {
			return nil, false, err
		}
		return inserted, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	existing, err := s.GetReplyByDelivery(reply.DeliveryID, reply.Target)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, fmt.Errorf("reply for delivery %s vanished mid-insert", reply.DeliveryID)
	}
	return existing, true, nil
}

func (s *Store) GetReply(id string) (*Reply, error) {
	reply, err := scanReply(s.db.QueryRow("SELECT * FROM replies WHERE id = ?", id))
	return noRows(reply, err)
}

func (s *Store) GetReplyByDelivery(deliveryID, target string) (*Reply, error) {
	reply, err := scanReply(s.db.QueryRow("SELECT * FROM replies WHERE delivery_id = ? AND target = ?", deliveryID, target))
	return noRows(reply, err)
}

func (s *Store) MarkPosted(replyID string, now int64) error {
	_, err := s.db.Exec("UPDATE replies SET posted_at = ?, post_error = NULL WHERE id = ?", now, replyID)
	return err
}

func (s *Store) MarkPostError(replyID, message string) error {
	_, err := s.db.Exec("UPDATE replies SET post_error = ? WHERE id = ?", truncateError(message), replyID)
	return err
}

func (s *Store) UnpostedReplies() ([]Reply, error) {
	rows, err := s.db.Query("SELECT * FROM replies WHERE posted_at IS NULL ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var replies []Reply
	for rows.Next() {
		reply, err := scanReply(rows)
		if err != nil {
			return nil, err
		}
		replies = append(replies, *reply)
	}
	return replies, rows.Err()
}

// handle is the only place that writes both settlement markers. Keep its two
// callers visible below: post confirmation and the explicit operator/tool door.
func (s *Store) handle(deliveryID string, now int64) bool {
	tx, err := s.db.Begin()
	if err != nil {
		panic(fmt.Errorf("begin handling delivery: %w", err))
	}
	defer tx.Rollback()
	var eventID int64
	if err := tx.QueryRow("SELECT event_id FROM deliveries WHERE id = ?", deliveryID).Scan(&eventID); errors.Is(err, sql.ErrNoRows) {
		return false
	} else if err != nil {
		panic(fmt.Errorf("load delivery for handling: %w", err))
	}
	result, err := tx.Exec("UPDATE deliveries SET status = 'handled' WHERE id = ? AND status != 'handled'", deliveryID)
	if err != nil {
		panic(fmt.Errorf("settle delivery: %w", err))
	}
	if _, err := tx.Exec("UPDATE events SET handled_at = ? WHERE id = ? AND handled_at IS NULL", now, eventID); err != nil {
		panic(fmt.Errorf("settle event: %w", err))
	}
	changed, err := result.RowsAffected()
	if err != nil {
		panic(fmt.Errorf("read settlement result: %w", err))
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Errorf("commit settlement: %w", err))
	}
	return changed > 0
}

func (s *Store) CompleteAfterPost(replyID string, now int64) bool {
	reply, err := s.GetReply(replyID)
	if err != nil {
		panic(fmt.Errorf("load reply before completion: %w", err))
	}
	if reply == nil {
		return false
	}
	if reply.PostedAt == nil {
		return false
	}
	if s.handle(reply.DeliveryID, now) {
		return true
	}
	// A connector may auto-settle the conversation after its outbound API
	// confirms the post but before MarkPosted records that confirmation. Treat
	// that already-handled delivery as successful completion, not a post error.
	delivery, err := s.GetDelivery(reply.DeliveryID)
	if err != nil {
		panic(fmt.Errorf("reload delivery after completion: %w", err))
	}
	return delivery != nil && delivery.Status == DeliveryHandled
}

type MarkHandledArgs struct {
	DeliveryID string
	EventID    *int64
}

func (s *Store) MarkHandled(args MarkHandledArgs, now int64) bool {
	if args.DeliveryID != "" {
		return s.handle(args.DeliveryID, now)
	}
	if args.EventID == nil {
		return false
	}
	open, err := s.OpenDeliveryForEvent(*args.EventID)
	if err != nil {
		panic(fmt.Errorf("resolve event delivery for handling: %w", err))
	}
	if open != nil {
		return s.handle(open.ID, now)
	}
	result, err := s.db.Exec("UPDATE events SET handled_at = ? WHERE id = ? AND handled_at IS NULL", now, *args.EventID)
	if err != nil {
		panic(fmt.Errorf("settle delivery-less event: %w", err))
	}
	changed, err := result.RowsAffected()
	if err != nil {
		panic(fmt.Errorf("read event settlement result: %w", err))
	}
	return changed > 0
}

func (s *Store) MarkConversationHandled(connector, conversationID string, beforeTS, now int64) (int, error) {
	rows, err := s.db.Query(`
        SELECT id FROM events
        WHERE connector = ? AND conversation_id = ? AND handled_at IS NULL AND received_at <= ?
        ORDER BY id ASC
      `, connector, conversationID, beforeTS)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	changed := 0
	for _, id := range ids {
		if s.MarkHandled(MarkHandledArgs{EventID: &id}, now) {
			changed++
		}
	}
	return changed, nil
}

func (s *Store) EventsAwaitingDelivery() ([]Event, error) {
	rows, err := s.db.Query(`
        SELECT e.* FROM events e
        WHERE e.handled_at IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM deliveries d WHERE d.event_id = e.id AND d.status != 'failed'
          )
        ORDER BY e.id ASC
      `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, rows.Err()
}

func (s *Store) Backfill(target string, now int64) ([]Delivery, error) {
	events, err := s.EventsAwaitingDelivery()
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery, 0, len(events))
	for _, event := range events {
		delivery, err := s.InsertDelivery(event.ID, target, now)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, *delivery)
	}
	return deliveries, nil
}

func scanReconcilerState(row rowScanner) (*ReconcilerState, error) {
	var state ReconcilerState
	var herdrSession, workspaceID, paneID sql.NullString
	var nativeSource, nativeKind, nativeValue sql.NullString
	if err := row.Scan(&state.OrgID, &herdrSession, &workspaceID, &paneID, &state.PaneLabel,
		&state.AgentKind, &nativeSource, &nativeKind, &nativeValue, &state.SessionGeneration, &state.UpdatedAt); err != nil {
		return nil, err
	}
	state.HerdrSession = nullableString(herdrSession)
	state.WorkspaceID = nullableString(workspaceID)
	state.PaneID = nullableString(paneID)
	state.NativeSessionSource = nullableString(nativeSource)
	state.NativeSessionKind = nullableString(nativeKind)
	state.NativeSessionValue = nullableString(nativeValue)
	return &state, nil
}

func (s *Store) GetReconcilerState(orgID string) (*ReconcilerState, error) {
	state, err := scanReconcilerState(s.db.QueryRow("SELECT * FROM reconciler_state WHERE org_id = ?", orgID))
	return noRows(state, err)
}

func (s *Store) PutReconcilerState(input ReconcilerStateInput, now int64) (*ReconcilerState, error) {
	generation := int64(0)
	if previous, err := s.GetReconcilerState(input.OrgID); err != nil {
		return nil, err
	} else if previous != nil {
		generation = previous.SessionGeneration
	}
	if input.SessionGeneration != nil {
		generation = *input.SessionGeneration
	}
	_, err := s.db.Exec(`
        INSERT INTO reconciler_state (org_id, herdr_session, workspace_id, pane_id, pane_label,
                                      agent_kind, native_session_source, native_session_kind,
                                      native_session_value, session_generation, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(org_id) DO UPDATE SET
          herdr_session = excluded.herdr_session,
          workspace_id = excluded.workspace_id,
          pane_id = excluded.pane_id,
          pane_label = excluded.pane_label,
          agent_kind = excluded.agent_kind,
          native_session_source = excluded.native_session_source,
          native_session_kind = excluded.native_session_kind,
          native_session_value = excluded.native_session_value,
          session_generation = excluded.session_generation,
          updated_at = excluded.updated_at
      `, input.OrgID, input.HerdrSession, input.WorkspaceID, input.PaneID, input.PaneLabel, input.AgentKind,
		input.NativeSessionSource, input.NativeSessionKind, input.NativeSessionValue, generation, now)
	if err != nil {
		return nil, err
	}
	return s.GetReconcilerState(input.OrgID)
}

func (s *Store) BumpGeneration(orgID string, now int64) (int64, error) {
	if _, err := s.db.Exec("UPDATE reconciler_state SET session_generation = session_generation + 1, updated_at = ? WHERE org_id = ?", now, orgID); err != nil {
		return 0, err
	}
	state, err := s.GetReconcilerState(orgID)
	if err != nil || state == nil {
		return 0, err
	}
	return state.SessionGeneration, nil
}

func (s *Store) GetSyncState(key string) (*int64, error) {
	var value int64
	if err := s.db.QueryRow("SELECT value FROM sync_state WHERE key = ?", key).Scan(&value); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &value, nil
}

func (s *Store) SetSyncState(key string, value int64) error {
	_, err := s.db.Exec(`
        INSERT INTO sync_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE excluded.value > value
      `, key, value)
	return err
}

func (s *Store) RecordPost(post PostInput, now int64) error {
	_, err := s.db.Exec(`
        INSERT INTO post_state (post_id, channel_id, message, edit_at, delete_at, updated_at)
        VALUES (?, ?, ?, ?, 0, ?)
        ON CONFLICT(post_id) DO UPDATE SET
          message = excluded.message, edit_at = excluded.edit_at, updated_at = excluded.updated_at
      `, post.PostID, post.ChannelID, post.Message, post.EditAt, now)
	return err
}

func (s *Store) GetPostState(postID string) (*PostState, error) {
	var post PostState
	err := s.db.QueryRow("SELECT * FROM post_state WHERE post_id = ?", postID).Scan(
		&post.PostID, &post.ChannelID, &post.Message, &post.EditAt, &post.DeleteAt, &post.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &post, err
}

func (s *Store) MarkPostDeleted(postID string, deleteAt, now int64) (bool, error) {
	result, err := s.db.Exec("UPDATE post_state SET delete_at = ?, updated_at = ? WHERE post_id = ?", deleteAt, now, postID)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed > 0, err
}
func (s *Store) FollowMattermostThread(channelID, rootID string, now int64) error {
	_, err := s.db.Exec(`
        INSERT INTO mattermost_threads (channel_id, root_id, followed_at)
        VALUES (?, ?, ?)
        ON CONFLICT(channel_id, root_id) DO NOTHING
      `, channelID, rootID, now)
	return err
}

func (s *Store) IsMattermostThreadFollowed(channelID, rootID string) (bool, error) {
	var followed int
	err := s.db.QueryRow(
		"SELECT 1 FROM mattermost_threads WHERE channel_id = ? AND root_id = ?",
		channelID, rootID,
	).Scan(&followed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) GetWatermark(account string) (*string, error) {
	var historyID string
	if err := s.db.QueryRow("SELECT history_id FROM watermarks WHERE account = ?", account).Scan(&historyID); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &historyID, nil
}

func (s *Store) SetWatermark(account, historyID string, now int64) error {
	_, err := s.db.Exec(`
        INSERT INTO watermarks (account, history_id, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(account) DO UPDATE SET history_id = excluded.history_id, updated_at = excluded.updated_at
      `, account, historyID, now)
	return err
}
