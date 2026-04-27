CREATE TABLE IF NOT EXISTS parcel_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  parcel_id INTEGER NOT NULL,
  alias_type TEXT NOT NULL,
  alias_value_raw TEXT NOT NULL,
  alias_value_norm TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 1.0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parcel_id) REFERENCES parcels(id) ON DELETE CASCADE,
  UNIQUE (alias_type, alias_value_norm)
);

CREATE INDEX IF NOT EXISTS idx_parcel_aliases_lookup
  ON parcel_aliases(alias_type, alias_value_norm);

CREATE TABLE IF NOT EXISTS opportunity_parcel_matches (
  opportunity_id TEXT PRIMARY KEY,
  parcel_id INTEGER NOT NULL,
  match_type TEXT NOT NULL,
  confidence REAL NOT NULL,
  input_value TEXT,
  matched_value TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parcel_id) REFERENCES parcels(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_opportunity_parcel_matches_parcel_id
  ON opportunity_parcel_matches(parcel_id);