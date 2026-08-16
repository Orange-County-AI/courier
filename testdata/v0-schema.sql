CREATE TABLE events (
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
);
CREATE UNIQUE INDEX idx_events_connector_key ON events(connector, event_key);
CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  event_id INTEGER NOT NULL REFERENCES events(id),
  target TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','dispatched','replied','handled','failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_dispatched_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_deliveries_one_open ON deliveries(event_id) WHERE status != 'failed';
CREATE TABLE replies (
  id TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL REFERENCES deliveries(id),
  target TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  posted_at INTEGER,
  post_error TEXT,
  UNIQUE(delivery_id, target)
);
CREATE TABLE reconciler_state (
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
);
