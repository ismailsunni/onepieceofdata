"""Post-processor to sync haki abilities from wiki categories.

Haki information is NOT stored in the character infobox — it lives only in
MediaWiki categories such as ``[[Category:Kenbunshoku Haki Users]]``.  This
module batch-fetches categories via the MediaWiki API and sets three boolean
columns on the ``character`` table.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Optional, Set

import duckdb
from loguru import logger

from ..api.fandom_client import FandomAPIClient

BATCH_SIZE = 50  # MediaWiki anonymous-user batch limit

# Category names → column names
HAKI_CATEGORIES: Dict[str, str] = {
    "Observation Haki Users": "haki_observation",
    "Armament Haki Users": "haki_armament",
    "Supreme King Haki Users": "haki_conqueror",
}


def _fetch_categories_batch(
    client: FandomAPIClient, titles: List[str]
) -> Dict[str, Set[str]]:
    """Return ``{title: {category_name, ...}}`` for a batch of page titles.

    Only categories in :data:`HAKI_CATEGORIES` are kept.
    """
    results: Dict[str, Set[str]] = {t: set() for t in titles}
    target_cats = set(HAKI_CATEGORIES.keys())
    # Filter server-side so the API only returns the 3 haki categories,
    # avoiding pagination issues when batching 50 titles at a time.
    cl_filter = "|".join(f"Category:{c}" for c in target_cats)

    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i : i + BATCH_SIZE]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "categories",
            "clcategories": cl_filter,
            "redirects": 1,
        }
        data = client._make_request(params)  # noqa: SLF001
        query = data.get("query", {})

        # Build normalization/redirect forward map (same as thumbnail script).
        forward: Dict[str, str] = {}
        for n in query.get("normalized", []) or []:
            forward[n["from"]] = n["to"]
        for r in query.get("redirects", []) or []:
            forward[r["from"]] = r["to"]

        def resolve(t: str) -> str:
            seen: set = set()
            while t in forward and t not in seen:
                seen.add(t)
                t = forward[t]
            return t

        # Index pages by title (API returns titles with spaces).
        by_title: Dict[str, list] = {}
        for _pid, page in (query.get("pages") or {}).items():
            title = page.get("title")
            if title:
                by_title[title] = page.get("categories") or []

        for t in batch:
            resolved = resolve(t)
            resolved_sp = resolved.replace("_", " ")
            cats = by_title.get(resolved_sp) or by_title.get(resolved) or []
            for cat in cats:
                cat_title = cat.get("title", "")
                # Strip "Category:" prefix
                if cat_title.startswith("Category:"):
                    cat_name = cat_title[len("Category:"):]
                    if cat_name in target_cats:
                        results[t].add(cat_name)

    return results


def sync_haki(
    db_path: str,
    *,
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Sync haki boolean columns on the character table from wiki categories.

    Creates the columns if they don't exist, then sets TRUE/FALSE per
    character based on category membership.

    Args:
        db_path: Path to the DuckDB database file.
        dry_run: If True, report what would change without writing.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Stats dict with total, observation, armament, conqueror counts.
    """
    conn = duckdb.connect(str(db_path))
    try:
        # Load all character IDs
        character_ids = [
            r[0] for r in conn.execute("SELECT id FROM character ORDER BY id").fetchall()
        ]
        total = len(character_ids)
        logger.info(f"Fetching haki categories for {total} characters...")

        if progress_callback:
            progress_callback(0, total, "fetching categories")

        client = FandomAPIClient()
        try:
            all_cats = _fetch_categories_batch(client, character_ids)
        finally:
            client.cleanup()

        stats = {
            "total": total,
            "haki_observation": 0,
            "haki_armament": 0,
            "haki_conqueror": 0,
            "any_haki": 0,
        }

        updates: list[tuple] = []
        for cid in character_ids:
            cats = all_cats.get(cid, set())
            obs = "Observation Haki Users" in cats
            arm = "Armament Haki Users" in cats
            con = "Supreme King Haki Users" in cats
            updates.append((obs, arm, con, cid))
            if obs:
                stats["haki_observation"] += 1
            if arm:
                stats["haki_armament"] += 1
            if con:
                stats["haki_conqueror"] += 1
            if obs or arm or con:
                stats["any_haki"] += 1

        logger.info(
            f"Haki users: {stats['any_haki']} total — "
            f"observation {stats['haki_observation']}, "
            f"armament {stats['haki_armament']}, "
            f"conqueror {stats['haki_conqueror']}"
        )

        if progress_callback:
            progress_callback(1, 2, "writing")

        if not dry_run:
            conn.executemany(
                """UPDATE character
                   SET haki_observation = ?,
                       haki_armament = ?,
                       haki_conqueror = ?
                   WHERE id = ?""",
                updates,
            )
            logger.success(f"Updated haki columns for {total} characters")

        if progress_callback:
            progress_callback(2, 2, "done")

        return stats
    finally:
        conn.close()
