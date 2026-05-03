CREATE TABLE IF NOT EXISTS opengov_sync_runs (
  id TEXT PRIMARY KEY,
  sync_type TEXT NOT NULL,
  source_community TEXT NOT NULL,
  status TEXT NOT NULL,
  fetched_count INTEGER DEFAULT 0,
  stored_count INTEGER DEFAULT 0,
  total_records INTEGER,
  pages_fetched INTEGER,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_opengov_sync_runs_type_started
ON opengov_sync_runs(sync_type, started_at DESC);
