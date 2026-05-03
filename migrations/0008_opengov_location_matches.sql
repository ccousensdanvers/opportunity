CREATE TABLE IF NOT EXISTS opengov_location_matches (
  id TEXT PRIMARY KEY,
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_title TEXT,
  discovered_address TEXT NOT NULL,
  normalized_address TEXT NOT NULL,
  location_id TEXT NOT NULL,
  match_score INTEGER NOT NULL,
  match_reason TEXT,
  location_type TEXT,
  street_no TEXT,
  street_name TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  mbl TEXT,
  mat_id TEXT,
  gis_id TEXT,
  owner_name TEXT,
  latitude REAL,
  longitude REAL,
  source_updated_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opengov_location_matches_location_id
ON opengov_location_matches(location_id);

CREATE INDEX IF NOT EXISTS idx_opengov_location_matches_source
ON opengov_location_matches(source_kind, source_id);

CREATE INDEX IF NOT EXISTS idx_opengov_location_matches_normalized_address
ON opengov_location_matches(normalized_address);
