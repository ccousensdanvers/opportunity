CREATE TABLE IF NOT EXISTS opengov_permit_records (
  record_key TEXT PRIMARY KEY,
  location_id TEXT,
  matched_address TEXT,
  site_address TEXT,
  permit_type TEXT,
  status TEXT,
  issued_date TEXT,
  permit_number TEXT,
  detail_url TEXT,
  applicant_name TEXT,
  source_community TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opengov_permit_records_location_id
  ON opengov_permit_records(location_id);
