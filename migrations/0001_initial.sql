CREATE TABLE IF NOT EXISTS parcels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  map_lot TEXT NOT NULL UNIQUE,
  address TEXT,
  owner_name TEXT,
  zoning_district TEXT,
  land_area_sqft INTEGER,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  parcel_id INTEGER,
  site_name TEXT,
  status TEXT NOT NULL DEFAULT 'watch',
  priority_score REAL NOT NULL DEFAULT 0,
  last_reviewed_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parcel_id) REFERENCES parcels(id)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  parcel_id INTEGER,
  source_name TEXT NOT NULL,
  source_record_id TEXT,
  event_type TEXT NOT NULL,
  event_date TEXT,
  summary TEXT NOT NULL,
  detail_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (parcel_id) REFERENCES parcels(id)
);

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  severity TEXT NOT NULL DEFAULT 'info',
  title TEXT NOT NULL,
  rationale TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reviewed_at TEXT,
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE TABLE IF NOT EXISTS notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  author TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trigger TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sites_status ON sites(status);
CREATE INDEX IF NOT EXISTS idx_events_parcel_id ON events(parcel_id);
CREATE INDEX IF NOT EXISTS idx_alerts_site_id ON alerts(site_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_requested_at ON ingestion_runs(requested_at);
