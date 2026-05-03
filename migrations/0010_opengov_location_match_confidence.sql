ALTER TABLE opengov_location_matches ADD COLUMN match_confidence TEXT;

UPDATE opengov_location_matches
SET match_confidence = CASE
  WHEN match_score >= 320 THEN 'high'
  WHEN match_score >= 260 THEN 'medium'
  ELSE 'low'
END
WHERE match_confidence IS NULL OR match_confidence = '';

CREATE INDEX IF NOT EXISTS idx_opengov_location_matches_confidence
ON opengov_location_matches(match_confidence);
