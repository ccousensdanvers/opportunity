#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:8787}"

echo
echo "Checking ingest readiness..."
curl -fsS "${BASE_URL}/ingest-info"

echo
echo
echo "Triggering ingest..."
curl -fsS -X POST "${BASE_URL}/ingest"

echo
echo
echo "Checking review queue..."
curl -fsS "${BASE_URL}/api/opportunities/review?limit=10"
echo
