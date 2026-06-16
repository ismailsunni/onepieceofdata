#!/usr/bin/env bash
#
# Publish the local DuckDB as a GitHub Release asset and keep only the latest N
# snapshots. GitHub Releases do NOT count against the Git LFS quota, so this
# replaces LFS as the place where the (regenerable) database is versioned.
#
# Usage:
#   scripts/release_db.sh [CHAPTER]
#
# CHAPTER defaults to OP_LAST_CHAPTER from .env. DB path comes from
# OP_DATABASE_PATH (default data/onepiece.duckdb).
#
# Tiered retention (everything else is pruned):
#   - the newest DB_RELEASE_KEEP releases          (default 3 — recent history)
#   - the newest release of each of the most       (default 6 — monthly archive,
#     recent DB_RELEASE_MONTHLY calendar months       i.e. last month, 2 months ago, ...)
set -euo pipefail

DB_PATH="${OP_DATABASE_PATH:-data/onepiece.duckdb}"
KEEP="${DB_RELEASE_KEEP:-3}"
MONTHLY="${DB_RELEASE_MONTHLY:-6}"
PREFIX="db-"

CHAPTER="${1:-}"
if [[ -z "$CHAPTER" && -f .env ]]; then
  CHAPTER="$(grep -E '^OP_LAST_CHAPTER=' .env | head -1 | cut -d= -f2 | tr -d '[:space:]')"
fi
[[ -n "$CHAPTER" ]] || { echo "❌ No chapter given and OP_LAST_CHAPTER not found in .env"; exit 1; }
[[ -f "$DB_PATH" ]] || { echo "❌ Database not found: $DB_PATH"; exit 1; }

TAG="${PREFIX}chapter-${CHAPTER}"
ASSET="onepiece-chapter-${CHAPTER}.duckdb.gz"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "🗜️  Compressing ${DB_PATH} → ${ASSET} ..."
gzip -c "$DB_PATH" > "${TMP}/${ASSET}"
echo "   size: $(du -h "${TMP}/${ASSET}" | cut -f1)"

if gh release view "$TAG" >/dev/null 2>&1; then
  echo "♻️  Updating existing release ${TAG} ..."
  gh release upload "$TAG" "${TMP}/${ASSET}" --clobber
else
  echo "🚀 Creating release ${TAG} ..."
  gh release create "$TAG" "${TMP}/${ASSET}" \
    --title "Database — Chapter ${CHAPTER}" \
    --notes "DuckDB snapshot at chapter ${CHAPTER}. Restore with: make restore-db"
fi

echo "🧹 Pruning ${PREFIX} releases (keeping newest ${KEEP} + ${MONTHLY} monthly) ..."
OLD_TAGS="$(gh release list --limit 500 --json tagName,createdAt --jq "
  [.[] | select(.tagName | startswith(\"${PREFIX}\"))] | sort_by(.createdAt) | reverse as \$all
  | (\$all[:${KEEP}] | map(.tagName)) as \$recent
  | (\$all | group_by(.createdAt[0:7]) | map(max_by(.createdAt))
     | sort_by(.createdAt) | reverse | .[:${MONTHLY}] | map(.tagName)) as \$monthly
  | ((\$recent + \$monthly) | unique) as \$keep
  | \$all | map(.tagName) | map(select(. as \$t | \$keep | index(\$t) | not)) | .[]
")"
if [[ -n "$OLD_TAGS" ]]; then
  while IFS= read -r old; do
    [[ -n "$old" ]] || continue
    echo "   🗑️  deleting ${old}"
    gh release delete "$old" --yes --cleanup-tag
  done <<< "$OLD_TAGS"
else
  echo "   nothing to prune"
fi

echo "✅ Database published as release ${TAG}"
