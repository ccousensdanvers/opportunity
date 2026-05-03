CREATE TABLE IF NOT EXISTS opengov_record_types (
  id TEXT NOT NULL,
  source_community TEXT NOT NULL,
  type TEXT,
  name TEXT,
  description TEXT,
  slug TEXT,
  category TEXT,
  module TEXT,
  active INTEGER,
  raw_json TEXT NOT NULL,
  source_updated_at TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (id, source_community)
);

CREATE INDEX IF NOT EXISTS idx_opengov_record_types_community
  ON opengov_record_types(source_community);

CREATE INDEX IF NOT EXISTS idx_opengov_record_types_category
  ON opengov_record_types(category);

CREATE INDEX IF NOT EXISTS idx_opengov_record_types_active
  ON opengov_record_types(active);
