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

    Implementation note: even with the ``clcategories`` filter, MediaWiki
    paginates the categories list across multiple responses when the total
    number of (page, category) pairs in the batch exceeds its internal
    limit (~10 per request for ``cllimit`` default). We must follow the
    ``continue`` token until exhausted, otherwise any page whose categories
    fall after the first-response cutoff is silently treated as having
    none — this is what was hiding Edward Newgate's three haki tags.
    """
    results: Dict[str, Set[str]] = {t: set() for t in titles}
    target_cats = set(HAKI_CATEGORIES.keys())
    cl_filter = "|".join(f"Category:{c}" for c in target_cats)

    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i : i + BATCH_SIZE]
        base_params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "categories",
            "clcategories": cl_filter,
            "cllimit": "max",
            "redirects": 1,
        }

        forward: Dict[str, str] = {}
        cats_by_title: Dict[str, Set[str]] = {}
        continue_token: Dict[str, str] = {}

        # Follow continuation tokens until the server says it's done.
        while True:
            params = {**base_params, **continue_token}
            data = client._make_request(params)  # noqa: SLF001
            query = data.get("query", {})

            for n in query.get("normalized", []) or []:
                forward[n["from"]] = n["to"]
            for r in query.get("redirects", []) or []:
                forward[r["from"]] = r["to"]

            for _pid, page in (query.get("pages") or {}).items():
                title = page.get("title")
                if not title:
                    continue
                bucket = cats_by_title.setdefault(title, set())
                for cat in page.get("categories") or []:
                    cat_title = cat.get("title", "")
                    if cat_title.startswith("Category:"):
                        bucket.add(cat_title[len("Category:"):])

            cont = data.get("continue")
            if not cont:
                break
            # Drop the bookkeeping "continue" key, keep e.g. "clcontinue".
            continue_token = {k: v for k, v in cont.items() if k != "continue"}

        def resolve(t: str) -> str:
            seen: set = set()
            while t in forward and t not in seen:
                seen.add(t)
                t = forward[t]
            return t

        for t in batch:
            resolved = resolve(t)
            resolved_sp = resolved.replace("_", " ")
            cats = cats_by_title.get(resolved_sp) or cats_by_title.get(resolved) or set()
            for cat_name in cats:
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
