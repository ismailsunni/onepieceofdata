"""Download character thumbnails from One Piece Wiki and upload to Supabase Storage.

For each character in DuckDB:
1. Fetch the default pageimage name via Fandom MediaWiki API (batched, 50 per request)
2. Try the manga variant (replace 'Anime' with 'Manga' in filename)
3. Get a 300px-wide thumbnail URL from the CDN
4. Upload to Supabase Storage bucket 'character-images' as {character_id}.webp

Usage:
    uv run python scripts/upload_character_thumbnails.py
    uv run python scripts/upload_character_thumbnails.py --force    # re-upload all
    uv run python scripts/upload_character_thumbnails.py --limit 10 # test with 10 chars
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse

import duckdb
from dotenv import load_dotenv

load_dotenv(".env")

FANDOM_API = "https://onepiece.fandom.com/api.php"
THUMB_WIDTH = 300
BATCH_SIZE = 50
REQUEST_DELAY = 1.0  # seconds between individual API calls
USER_AGENT = "OnePieceOfData/2.0 (character-thumbnails)"


def get_characters(db_path: str, limit: int = 0) -> list[dict]:
    """Load character IDs and names from DuckDB."""
    conn = duckdb.connect(db_path, read_only=True)
    query = "SELECT id, name FROM character WHERE is_likely_character = true ORDER BY id"
    if limit > 0:
        query += f" LIMIT {limit}"
    rows = conn.execute(query).fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1]} for r in rows]


def api_request(params: dict) -> dict:
    """Make a request to the Fandom MediaWiki API."""
    params["format"] = "json"
    url = f"{FANDOM_API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def batch_fetch_pageimages(character_ids: list[str]) -> dict[str, str]:
    """Fetch pageimage filenames for characters in batches of 50.

    Returns a dict mapping character_id -> pageimage filename.
    Uses &redirects to handle alias pages.
    """
    result = {}
    for i in range(0, len(character_ids), BATCH_SIZE):
        batch = character_ids[i : i + BATCH_SIZE]
        titles = "|".join(batch)
        data = api_request(
            {"action": "query", "titles": titles, "redirects": "", "prop": "pageimages"}
        )

        # Build redirect map: original title -> final title
        redirects = {}
        for r in data.get("query", {}).get("normalized", []):
            redirects[r["from"]] = r["to"]
        for r in data.get("query", {}).get("redirects", []):
            # Chain: if A was normalized to B, then B redirected to C
            for orig, norm in list(redirects.items()):
                if norm == r["from"]:
                    redirects[orig] = r["to"]
            redirects[r["from"]] = r["to"]

        # Map page titles back to character IDs
        title_to_id = {}
        for cid in batch:
            # The API normalizes underscores to spaces
            normalized = cid.replace("_", " ")
            final_title = redirects.get(cid, redirects.get(normalized, normalized))
            title_to_id[final_title] = cid

        for page in data.get("query", {}).get("pages", {}).values():
            title = page.get("title", "")
            pageimage = page.get("pageimage", "")
            cid = title_to_id.get(title)
            if cid and pageimage:
                result[cid] = pageimage

        if i + BATCH_SIZE < len(character_ids):
            time.sleep(REQUEST_DELAY)

    return result


def resolve_manga_thumbnail(pageimage: str) -> tuple[str, str]:
    """Try to get the manga variant of a pageimage, fall back to original.

    Returns (thumbnail_url, variant_used) where variant_used is 'manga' or 'anime'.
    """
    manga_image = pageimage.replace("Anime", "Manga")
    candidates = []
    if manga_image != pageimage:
        candidates.append(("manga", manga_image))
    candidates.append(("original", pageimage))

    for variant, filename in candidates:
        data = api_request(
            {
                "action": "query",
                "titles": f"File:{filename}",
                "prop": "imageinfo",
                "iiprop": "url|size",
                "iiurlwidth": str(THUMB_WIDTH),
            }
        )
        for page in data.get("query", {}).get("pages", {}).values():
            if int(page.get("pageid", -1)) < 0:
                continue  # File doesn't exist
            info = page.get("imageinfo", [{}])[0]
            thumb_url = info.get("thumburl") or info.get("url")
            if thumb_url:
                return (thumb_url, variant)

    return ("", "none")


def download_image(url: str) -> bytes:
    """Download an image from a URL."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    resp = urllib.request.urlopen(req, timeout=30)
    return resp.read()


def list_existing_images(supabase_url: str, service_key: str, bucket: str) -> set[str]:
    """List all existing files in the Supabase Storage bucket."""
    existing = set()
    offset = 0
    limit = 1000

    while True:
        url = f"{supabase_url}/storage/v1/object/list/{bucket}"
        body = json.dumps({"prefix": "", "limit": limit, "offset": offset}).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {service_key}",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            items = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"  Bucket '{bucket}' not found. Create it in Supabase dashboard first.")
                sys.exit(1)
            raise

        if not items:
            break

        for item in items:
            name = item.get("name", "")
            if name:
                existing.add(name)

        if len(items) < limit:
            break
        offset += limit

    return existing


def upload_image(
    supabase_url: str, service_key: str, bucket: str, path: str, data: bytes
) -> bool:
    """Upload an image to Supabase Storage. Returns True on success."""
    url = f"{supabase_url}/storage/v1/object/{bucket}/{path}"
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "image/webp",
            "x-upsert": "true",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=60)
        return True
    except urllib.error.HTTPError as e:
        print(f"  Upload failed ({e.code}): {e.read().decode()}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload character thumbnails to Supabase Storage")
    parser.add_argument("--force", action="store_true", help="Re-upload all images (ignore existing)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of characters (for testing)")
    parser.add_argument("--db", default="data/onepiece.duckdb", help="Path to DuckDB database")
    parser.add_argument("--bucket", default="character-images", help="Supabase Storage bucket name")
    args = parser.parse_args()

    supabase_url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not service_key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        sys.exit(1)

    # Step 1: Load characters
    print("Loading characters from DuckDB...")
    characters = get_characters(args.db, args.limit)
    print(f"  Found {len(characters)} characters")

    # Step 2: Check existing images in Supabase
    existing = set()
    if not args.force:
        print("Checking existing images in Supabase Storage...")
        existing = list_existing_images(supabase_url, service_key, args.bucket)
        print(f"  Found {len(existing)} existing images")

    # Step 3: Batch fetch pageimage names
    print("Fetching pageimage names from Fandom API...")
    character_ids = [c["id"] for c in characters]
    pageimages = batch_fetch_pageimages(character_ids)
    print(f"  Got pageimages for {len(pageimages)}/{len(characters)} characters")

    # Step 4: Download and upload
    uploaded = 0
    skipped = 0
    failed = 0
    no_image = []

    for i, char in enumerate(characters):
        cid = char["id"]
        name = char["name"]
        filename = f"{cid}.webp"
        progress = f"[{i + 1}/{len(characters)}]"

        # Skip if already uploaded
        if filename in existing:
            skipped += 1
            continue

        # Skip if no pageimage found
        pageimage = pageimages.get(cid)
        if not pageimage:
            no_image.append(f"{cid} ({name})")
            continue

        # Resolve manga variant and get thumbnail URL
        thumb_url, variant = resolve_manga_thumbnail(pageimage)
        if not thumb_url:
            no_image.append(f"{cid} ({name}) - no thumbnail URL")
            continue

        # Download
        try:
            image_data = download_image(thumb_url)
        except Exception as e:
            print(f"  {progress} FAIL download {name}: {e}")
            failed += 1
            time.sleep(REQUEST_DELAY)
            continue

        # Upload
        if upload_image(supabase_url, service_key, args.bucket, filename, image_data):
            uploaded += 1
            print(f"  {progress} OK {name} ({variant}, {len(image_data) // 1024} KB)")
        else:
            failed += 1

        time.sleep(REQUEST_DELAY)

    # Summary
    print()
    print("=" * 60)
    print(f"Done! Uploaded: {uploaded}, Skipped: {skipped}, Failed: {failed}")
    if no_image:
        print(f"\nNo image found for {len(no_image)} characters:")
        for entry in no_image:
            print(f"  - {entry}")


if __name__ == "__main__":
    main()
