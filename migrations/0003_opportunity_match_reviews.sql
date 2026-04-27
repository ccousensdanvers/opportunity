CREATE TABLE IF NOT EXISTS opportunity_parcel_matches_v2 (
  opportunity_id TEXT PRIMARY KEY,
  parcel_id INTEGER,
  match_type TEXT NOT NULL,
  confidence REAL NOT NULL,
  input_value TEXT,
  matched_value TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0,
  raw_input_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parcel_id) REFERENCES parcels(id) ON DELETE SET NULL
);

INSERT INTO opportunity_parcel_matches_v2 (
  opportunity_id,
  parcel_id,
  match_type,
  confidence,
  input_value,
  matched_value,
  needs_review,
  raw_input_json,
  created_at,
  updated_at
)
SELECT
  opportunity_id,
  parcel_id,
  match_type,
  confidence,
  input_value,
  matched_value,
  needs_review,
  json_object(
    'id', opportunity_id,
    'mapLot', CASE WHEN match_type = 'map_lot_exact' THEN input_value ELSE NULL END,
    'address', CASE WHEN match_type IN ('address_exact', 'address_prefix') THEN input_value ELSE NULL END,
    'ownerName', CASE WHEN match_type = 'owner_exact' THEN input_value ELSE NULL END
  ),
  created_at,
  updated_at
FROM opportunity_parcel_matches;

DROP TABLE opportunity_parcel_matches;

ALTER TABLE opportunity_parcel_matches_v2 RENAME TO opportunity_parcel_matches;

CREATE INDEX IF NOT EXISTS idx_opportunity_parcel_matches_parcel_id
  ON opportunity_parcel_matches(parcel_id);

CREATE INDEX IF NOT EXISTS idx_opportunity_parcel_matches_review
  ON opportunity_parcel_matches(needs_review, confidence, updated_at);
