"""Upload poll character face images to Supabase Storage.

Images go under a `polls/<poll_id>/` prefix inside the thumbnail bucket so they
stay separate from the character thumbnails that live at the bucket root, and
the object path is written back to `character_poll.image_path` for the web app.

Idempotent: objects already present in the bucket are skipped unless --force.

    uv run python scripts/upload_poll_images.py --poll wt100_2026 [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from onepieceofdata.config.settings import get_settings  # noqa: E402

DB = REPO_ROOT / "data" / "onepiece.duckdb"
LOCAL_DIRS = {"wt100_2026": REPO_ROOT / "data" / "wt100_2026_images"}


def list_existing(supabase, bucket: str, prefix: str) -> set[str]:
    """Return object names already present under prefix (paginated)."""
    names: set[str] = set()
    page, page_size = 0, 1000
    while True:
        res = supabase.storage.from_(bucket).list(
            path=prefix, options={"limit": page_size, "offset": page * page_size}
        )
        if not res:
            break
        names.update(obj["name"] for obj in res if obj.get("name"))
        if len(res) < page_size:
            break
        page += 1
    return names


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--poll", default="wt100_2026", help="poll_id to upload images for")
    parser.add_argument("--bucket", default=None, help="Supabase Storage bucket (default: from settings)")
    parser.add_argument("--dry-run", action="store_true", help="report what would be uploaded")
    parser.add_argument("--force", action="store_true", help="re-upload objects that already exist")
    args = parser.parse_args()

    settings = get_settings()
    bucket = args.bucket or settings.supabase_thumbnail_bucket
    prefix = f"polls/{args.poll}"
    local_dir = LOCAL_DIRS.get(args.poll)
    if not local_dir or not local_dir.is_dir():
        raise SystemExit(f"No local image directory for poll '{args.poll}' (run the scraper first)")

    con = duckdb.connect(str(DB))
    rows = con.execute(
        "SELECT rank, name, image_path FROM character_poll WHERE poll_id = ? AND image_path IS NOT NULL ORDER BY rank",
        [args.poll],
    ).fetchall()
    if not rows:
        raise SystemExit(f"No rows with an image_path for poll '{args.poll}'")

    from supabase import create_client

    if not settings.supabase_url or not settings.supabase_service_key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
    supabase = create_client(settings.supabase_url, settings.supabase_service_key)

    existing = set() if args.force else list_existing(supabase, bucket, prefix)
    logger.info(f"{len(rows)} images to consider, {len(existing)} already in {bucket}/{prefix}")

    uploaded = skipped = missing = failed = 0
    for rank, name, path in rows:
        filename = path.rsplit("/", 1)[-1]
        local = local_dir / filename
        if not local.exists():
            logger.warning(f"#{rank} {name}: missing local file {filename}")
            missing += 1
            continue
        if filename in existing:
            skipped += 1
            continue
        if args.dry_run:
            logger.info(f"DRY #{rank} {name} -> {bucket}/{path}")
            uploaded += 1
            continue
        try:
            supabase.storage.from_(bucket).upload(
                path=path,
                file=local.read_bytes(),
                file_options={"content-type": "image/png", "upsert": "true" if args.force else "false"},
            )
            uploaded += 1
            if uploaded % 100 == 0:
                logger.info(f"uploaded {uploaded}...")
        except Exception as e:  # noqa: BLE001
            logger.error(f"#{rank} {name}: {e}")
            failed += 1

    verb = "would upload" if args.dry_run else "uploaded"
    logger.info(f"{verb} {uploaded}, skipped {skipped}, missing local {missing}, failed {failed}")


if __name__ == "__main__":
    main()
