#!/usr/bin/env bash
#
# Download the latest DuckDB snapshot from GitHub Releases and restore it to the
# local database path. Counterpart to scripts/release_db.sh.
#
# Usage:
#   scripts/restore_db.sh [TAG]
#
# TAG defaults to the most recent db-* release. DB path comes from
# OP_DATABASE_PATH (default data/onepiece.duckdb). An existing DB is backed up
# to <db>.bak before being overwritten.
set -euo pipefail

DB_PATH="${OP_DATABASE_PATH:-data/onepiece.duckdb}"
PREFIX="db-"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  TAG="$(gh release list --limit 200 --json tagName,createdAt \
    --jq "[.[] | select(.tagName | startswith(\"${PREFIX}\"))] | sort_by(.createdAt) | reverse | .[0].tagName")"
fi
[[ -n "$TAG" ]] || { echo "❌ No ${PREFIX} release found"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "⬇️  Downloading database from release ${TAG} ..."
gh release download "$TAG" --pattern '*.duckdb.gz' --dir "$TMP"
GZ="$(ls "${TMP}"/*.duckdb.gz | head -1)"
[[ -f "$GZ" ]] || { echo "❌ No .duckdb.gz asset in ${TAG}"; exit 1; }

mkdir -p "$(dirname "$DB_PATH")"
if [[ -f "$DB_PATH" ]]; then
  echo "💾 Backing up existing DB → ${DB_PATH}.bak"
  mv "$DB_PATH" "${DB_PATH}.bak"
fi

echo "📦 Decompressing → ${DB_PATH} ..."
gunzip -c "$GZ" > "$DB_PATH"
echo "✅ Restored ${DB_PATH} from ${TAG} ($(du -h "$DB_PATH" | cut -f1))"
