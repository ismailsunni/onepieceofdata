"""Download character images from the One Piece wiki and upload them to
Supabase Storage as small thumbnails for the frontend.

Flow:
  1. Load character IDs from DuckDB.
  2. Batch-call MediaWiki ``pageimages`` to get the default infobox filename
     for every character (50 titles per request).
  3. For each character, compute the manga-variant filename by swapping
     "Anime" for "Manga" in the pageimage name. Batch ``imageinfo`` on both
     variants at once; pick manga if it exists, otherwise anime, otherwise
     the pageimage filename as-is (non-standard naming).
  4. List the target Supabase bucket once; skip characters whose object
     already exists (idempotent).
  5. Download the CDN-resized thumbnail at the configured width and upload
     it to Supabase Storage under ``{character_id}.{ext}``.

Re-run safely — existing uploads are skipped unless ``--force`` is passed.
"""

from __future__ import annotations

import argparse
import mimetypes
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import duckdb
import urllib3
from loguru import logger

# Make `src/` importable when running as a script.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from onepieceofdata.api.fandom_client import FandomAPIClient  # noqa: E402
from onepieceofdata.config.settings import get_settings  # noqa: E402


DEFAULT_THUMB_WIDTH = 150
BATCH_SIZE = 50  # MediaWiki anonymous API limit


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #


@dataclass
class ThumbnailJob:
    """Plan for a single character's thumbnail upload."""

    character_id: str
    # Title used against MediaWiki (underscores → spaces is handled by API).
    wiki_title: str
    # Original pageimage (anime variant, usually).
    pageimage: Optional[str] = None
    # Resolved filename we're actually downloading (manga preferred).
    chosen_filename: Optional[str] = None
    # CDN thumb URL at the requested width.
    thumb_url: Optional[str] = None
    # Why this variant was picked (manga/anime/as-is/none).
    variant: str = "none"
    # Reason for skipping/failing, if any.
    error: Optional[str] = None
    # Storage path written (when successful).
    uploaded_path: Optional[str] = None


@dataclass
class RunStats:
    total: int = 0
    already_uploaded: int = 0
    no_pageimage: int = 0
    uploaded: int = 0
    failed: int = 0
    variants: Dict[str, int] = field(default_factory=dict)

    def bump_variant(self, variant: str) -> None:
        self.variants[variant] = self.variants.get(variant, 0) + 1


# --------------------------------------------------------------------------- #
# MediaWiki helpers (extend FandomAPIClient inline; keeps the client stable)
# --------------------------------------------------------------------------- #


def get_pageimages(
    client: FandomAPIClient, titles: List[str]
) -> Dict[str, Optional[str]]:
    """Return ``{title: pageimage_filename}`` for a batch of titles.

    Handles the 50-title batch limit and normalizes the API's
    redirect/normalization maps so the caller's original title is the key.
    """
    results: Dict[str, Optional[str]] = {t: None for t in titles}

    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i : i + BATCH_SIZE]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "pageimages",
            "piprop": "name",
            "redirects": 1,
        }
        data = client._make_request(params)  # noqa: SLF001 — reuse client plumbing
        query = data.get("query", {})

        # Normalization / redirect maps tell us how our input title was
        # rewritten. We need to walk from our input → normalized → redirect
        # target to find the final page in ``pages``.
        forward: Dict[str, str] = {}
        for n in query.get("normalized", []) or []:
            forward[n["from"]] = n["to"]
        for r in query.get("redirects", []) or []:
            forward[r["from"]] = r["to"]

        def resolve(t: str) -> str:
            seen = set()
            while t in forward and t not in seen:
                seen.add(t)
                t = forward[t]
            return t

        pages = query.get("pages", {}) or {}
        # Build a lookup from resolved title → pageimage
        by_title: Dict[str, Optional[str]] = {}
        for _page_id, page in pages.items():
            title = page.get("title")
            if not title:
                continue
            by_title[title] = page.get("pageimage")

        for t in batch:
            resolved = resolve(t)
            # Wiki API uses spaces in titles; our DB uses underscores.
            resolved_space = resolved.replace("_", " ")
            results[t] = by_title.get(resolved_space) or by_title.get(resolved)

    return results


def get_imageinfo(
    client: FandomAPIClient, filenames: Iterable[str], thumb_width: int
) -> Dict[str, dict]:
    """Return ``{filename: {'missing': bool, 'thumburl': str|None, 'mime': str|None}}``.

    Filenames are sent with the ``File:`` prefix; keys in the returned dict
    are the plain filenames (no prefix) for easier lookup.
    """
    filenames = [f for f in filenames if f]
    results: Dict[str, dict] = {}

    for i in range(0, len(filenames), BATCH_SIZE):
        batch = filenames[i : i + BATCH_SIZE]
        titles = "|".join(f"File:{f}" for f in batch)
        params = {
            "action": "query",
            "titles": titles,
            "prop": "imageinfo",
            "iiprop": "url|size|mime",
            "iiurlwidth": thumb_width,
        }
        data = client._make_request(params)  # noqa: SLF001
        pages = data.get("query", {}).get("pages", {}) or {}
        for _page_id, page in pages.items():
            title = page.get("title", "")  # "File:Foo bar.png"
            fname = title.split(":", 1)[1] if ":" in title else title
            # MediaWiki returns titles with spaces; our cache keys use
            # underscores so lookups round-trip against the pageimage name.
            fname = fname.replace(" ", "_")
            if "missing" in page:
                results[fname] = {"missing": True, "thumburl": None, "mime": None}
                continue
            info = (page.get("imageinfo") or [{}])[0]
            results[fname] = {
                "missing": False,
                "thumburl": info.get("thumburl"),
                "mime": info.get("thumbmime") or info.get("mime"),
            }

        # Back-fill anything the API didn't mention (shouldn't happen, but be safe).
        for f in batch:
            results.setdefault(f, {"missing": True, "thumburl": None, "mime": None})

    return results


# --------------------------------------------------------------------------- #
# Variant resolution
# --------------------------------------------------------------------------- #


def manga_variant(filename: str) -> Optional[str]:
    """Return the manga-variant filename if the name follows the standard
    pattern, else None. Common patterns:

      - ``X_Anime_Post_Timeskip_Infobox.png`` → ``X_Manga_Post_Timeskip_Infobox.png``
      - ``X_Anime_Infobox.png`` → ``X_Manga_Infobox.png``
    """
    if not filename or "Anime" not in filename:
        return None
    # Replace only the last occurrence of "Anime" to be safe when a name
    # happens to contain "Anime" elsewhere.
    head, sep, tail = filename.rpartition("Anime")
    return f"{head}Manga{tail}" if sep else None


# --------------------------------------------------------------------------- #
# Supabase helpers
# --------------------------------------------------------------------------- #


def build_supabase_client(settings):
    """Lazily construct a supabase-py client; raises if env vars are missing."""
    from supabase import create_client  # deferred import so --dry-run works bare

    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env "
            "before uploading (see .env.example)."
        )
    return create_client(settings.supabase_url, settings.supabase_service_key)


def list_existing_objects(supabase, bucket: str) -> Dict[str, int]:
    """Return ``{name: size}`` for all objects currently in the bucket.

    Paginates through 1k-object pages (Supabase Storage default limit).
    """
    existing: Dict[str, int] = {}
    page = 0
    page_size = 1000
    while True:
        res = supabase.storage.from_(bucket).list(
            path="",
            options={"limit": page_size, "offset": page * page_size},
        )
        if not res:
            break
        for obj in res:
            name = obj.get("name")
            if name:
                existing[name] = (obj.get("metadata") or {}).get("size", 0)
        if len(res) < page_size:
            break
        page += 1
    return existing


def extension_from_mime(mime: Optional[str], fallback_url: Optional[str]) -> str:
    """Prefer the declared MIME type; fall back to the URL's extension."""
    if mime:
        guess = mimetypes.guess_extension(mime.split(";")[0].strip())
        if guess:
            return guess
    if fallback_url:
        path = fallback_url.split("?")[0]
        if "." in path.rsplit("/", 1)[-1]:
            return "." + path.rsplit(".", 1)[-1]
    return ".webp"


# --------------------------------------------------------------------------- #
# Core pipeline
# --------------------------------------------------------------------------- #


def load_character_ids(db_path: Path, limit: Optional[int]) -> List[str]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        query = "SELECT id FROM character ORDER BY id"
        if limit:
            query += f" LIMIT {int(limit)}"
        return [row[0] for row in conn.execute(query).fetchall()]
    finally:
        conn.close()


def plan_jobs(
    client: FandomAPIClient,
    character_ids: List[str],
    thumb_width: int,
) -> List[ThumbnailJob]:
    """Resolve a chosen filename + thumb URL for every character."""
    # Wiki API accepts underscores or spaces for page titles; we send IDs
    # verbatim (they're already underscored).
    jobs: Dict[str, ThumbnailJob] = {
        cid: ThumbnailJob(character_id=cid, wiki_title=cid) for cid in character_ids
    }

    logger.info(f"Fetching pageimages for {len(character_ids)} characters...")
    pageimages = get_pageimages(client, character_ids)
    for cid, job in jobs.items():
        job.pageimage = pageimages.get(cid)
        if not job.pageimage:
            job.error = "no pageimage"

    # Build the combined filename list (anime + manga variant).
    to_check: List[str] = []
    for job in jobs.values():
        if not job.pageimage:
            continue
        to_check.append(job.pageimage)
        variant = manga_variant(job.pageimage)
        if variant and variant != job.pageimage:
            to_check.append(variant)

    logger.info(f"Resolving imageinfo for {len(to_check)} filenames...")
    info = get_imageinfo(client, to_check, thumb_width)

    # Select the variant per character.
    for job in jobs.values():
        if not job.pageimage:
            continue
        manga_name = manga_variant(job.pageimage)
        manga_info = info.get(manga_name) if manga_name else None
        anime_info = info.get(job.pageimage)

        if manga_info and not manga_info["missing"] and manga_info["thumburl"]:
            job.chosen_filename = manga_name
            job.thumb_url = manga_info["thumburl"]
            job.variant = "manga"
            job._mime = manga_info["mime"]  # type: ignore[attr-defined]
        elif anime_info and not anime_info["missing"] and anime_info["thumburl"]:
            job.chosen_filename = job.pageimage
            job.thumb_url = anime_info["thumburl"]
            # If the pageimage name contained no "Anime", report variant as "as-is".
            job.variant = "anime" if "Anime" in job.pageimage else "as-is"
            job._mime = anime_info["mime"]  # type: ignore[attr-defined]
        else:
            job.error = "no usable imageinfo"

    return list(jobs.values())


def download(http: urllib3.PoolManager, url: str, attempts: int = 3) -> bytes:
    """Download a URL with a small retry loop."""
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = http.request(
                "GET",
                url,
                headers={
                    "User-Agent": "OnePieceOfData/2.0 (thumbnail sync)",
                    "Accept": "image/webp,image/*;q=0.8",
                },
                timeout=30,
            )
            if r.status == 200:
                return r.data
            last_err = Exception(f"HTTP {r.status}")
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(1 + i)
    raise last_err or Exception("download failed")


def run(
    db_path: Path,
    *,
    bucket: str,
    thumb_width: int = DEFAULT_THUMB_WIDTH,
    limit: Optional[int] = None,
    dry_run: bool = False,
    force: bool = False,
) -> RunStats:
    settings = get_settings()
    stats = RunStats()

    character_ids = load_character_ids(db_path, limit)
    stats.total = len(character_ids)
    logger.info(f"Loaded {stats.total} character IDs from {db_path}")

    client = FandomAPIClient()
    try:
        jobs = plan_jobs(client, character_ids, thumb_width)
    finally:
        client.cleanup()

    # Tally variants early so the dry-run report is useful.
    for job in jobs:
        if not job.thumb_url:
            stats.no_pageimage += 1
            continue
        stats.bump_variant(job.variant)

    supabase = None
    existing: Dict[str, int] = {}
    if not dry_run:
        supabase = build_supabase_client(settings)
        logger.info(f"Listing existing objects in bucket '{bucket}'...")
        existing = list_existing_objects(supabase, bucket)
        logger.info(f"Bucket has {len(existing)} existing objects")

    http = urllib3.PoolManager()
    try:
        for i, job in enumerate(jobs, 1):
            if not job.thumb_url:
                # Already counted as no_pageimage or errored above.
                logger.debug(f"[{i}/{len(jobs)}] {job.character_id}: {job.error}")
                continue

            mime = getattr(job, "_mime", None)
            ext = extension_from_mime(mime, job.thumb_url)
            object_path = f"{job.character_id}{ext}"

            if not force and object_path in existing:
                stats.already_uploaded += 1
                logger.debug(f"[{i}/{len(jobs)}] {job.character_id}: skip (exists)")
                continue

            if dry_run:
                logger.info(
                    f"[{i}/{len(jobs)}] DRY {job.character_id} "
                    f"({job.variant}) -> {object_path}  {job.thumb_url}"
                )
                continue

            try:
                payload = download(http, job.thumb_url)
                supabase.storage.from_(bucket).upload(
                    path=object_path,
                    file=payload,
                    file_options={
                        "content-type": mime or "image/webp",
                        "upsert": "true" if force else "false",
                    },
                )
                job.uploaded_path = object_path
                stats.uploaded += 1
                logger.info(
                    f"[{i}/{len(jobs)}] ✓ {job.character_id} "
                    f"({job.variant}, {len(payload)/1024:.1f} KB) -> {object_path}"
                )
            except Exception as e:  # noqa: BLE001
                stats.failed += 1
                job.error = str(e)
                logger.error(f"[{i}/{len(jobs)}] ✗ {job.character_id}: {e}")
    finally:
        http.clear()

    _report(stats, dry_run=dry_run)
    return stats


def _report(stats: RunStats, *, dry_run: bool) -> None:
    logger.info("─────── Summary ───────")
    logger.info(f"Total characters:   {stats.total}")
    logger.info(f"No pageimage:       {stats.no_pageimage}")
    logger.info(f"Already uploaded:   {stats.already_uploaded}")
    if dry_run:
        eligible = stats.total - stats.no_pageimage - stats.already_uploaded
        logger.info(f"Would upload:       {eligible}")
    else:
        logger.info(f"Uploaded:           {stats.uploaded}")
        logger.info(f"Failed:             {stats.failed}")
    if stats.variants:
        logger.info("Variant breakdown:")
        for variant, n in sorted(stats.variants.items(), key=lambda x: -x[1]):
            logger.info(f"  {variant}: {n}")


# --------------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------------- #


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    settings = get_settings()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db",
        type=Path,
        default=settings.database_path,
        help="DuckDB database path",
    )
    p.add_argument(
        "--bucket",
        default=settings.supabase_thumbnail_bucket,
        help="Supabase Storage bucket name",
    )
    p.add_argument(
        "--width",
        type=int,
        default=DEFAULT_THUMB_WIDTH,
        help="Thumbnail width in pixels",
    )
    p.add_argument("--limit", type=int, help="Only process the first N characters")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve images but do not download or upload anything",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-upload objects even when the target path already exists",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        run(
            db_path=args.db,
            bucket=args.bucket,
            thumb_width=args.width,
            limit=args.limit,
            dry_run=args.dry_run,
            force=args.force,
        )
        return 0
    except Exception as e:  # noqa: BLE001
        logger.exception(f"Upload failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
