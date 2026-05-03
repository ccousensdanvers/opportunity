CREATE TABLE IF NOT EXISTS opengov_locations (
  id TEXT PRIMARY KEY,
  location_type TEXT,
  street_no TEXT,
  street_name TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  gis_id TEXT,
  mbl TEXT,
  mat_id TEXT,
  source_community TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opengov_locations_address
  ON opengov_locations(street_no, street_name, city, state, postal_code);
