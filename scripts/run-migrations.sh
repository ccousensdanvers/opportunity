#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-local}"

if ! command -v wrangler >/dev/null 2>&1; then
  echo "wrangler is not installed or not on PATH."
  echo "Install it in your normal environment, then rerun this script."
  exit 1
fi

case "$MODE" in
  local)
    wrangler d1 migrations apply OPPORTUNITY --local
    ;;
  remote)
    wrangler d1 migrations apply OPPORTUNITY --remote
    ;;
  *)
    echo "Usage: $0 [local|remote]"
    exit 1
    ;;
esac
