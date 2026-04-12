# Plan: Character Thumbnail Images

Download character images from the One Piece Wiki and upload to Supabase Storage as small thumbnails for the React frontend at onepieceofdata.com.

## Investigation Findings

### Image API

The Fandom MediaWiki `pageimages` API is the best entry point:

```
https://onepiece.fandom.com/api.php?action=query&titles=Monkey_D._Luffy&prop=pageimages&format=json
```

- Returns the default infobox image filename (e.g. `Monkey_D._Luffy_Anime_Post_Timeskip_Infobox.png`)
- Supports `&redirects` to handle alias pages (e.g. Klahadore → Kuro)
- Supports batching up to 50 titles per request — ~29 calls for all 1,444 characters

### Manga vs Anime Image Selection

The wiki infobox naming convention is consistent:

- `{Name}_Anime_Post_Timeskip_Infobox.png` → `{Name}_Manga_Post_Timeskip_Infobox.png`
- `{Name}_Anime_Infobox.png` → `{Name}_Manga_Infobox.png`

**Strategy**: Get `pageimage` (always returns anime version), replace `Anime` with `Manga` in the filename, verify it exists via `imageinfo` API. Fall back to anime if manga version doesn't exist.

**Availability** (tested across 90 random characters):

- 100% have a `pageimage`
- ~97% follow the `Anime`/`Manga` naming pattern
- 100% of those with the pattern have a manga version available
- ~3% have non-standard names (e.g. `Goa_Kingdom_Infobox.png`) — use as-is

### Thumbnail Sizing — No Pillow Needed

The Fandom CDN does server-side resizing. The `imageinfo` API with `iiprop=url|size&iiurlwidth=150` returns a `thumburl` at the requested width. The CDN returns pre-resized webp images — no need for Pillow.

### Size Estimates

| Width | Per image | Total (1,444 chars) |
|-------|-----------|---------------------|
| 100px | ~11 KB    | ~15 MB              |
| 150px | ~23 KB    | ~32 MB              |
| 200px | ~38 KB    | ~54 MB              |

**Recommendation**: 150px width. All sizes are well within Supabase free tier (1 GB).

### Character ID Mapping

Character IDs in DuckDB (e.g. `Monkey_D._Luffy`) map directly to wiki page titles. The API normalizes underscores automatically. No separate URL field needed.

### Dependencies

- `supabase` (supabase-py) — needed for Storage API. Not currently installed.
- Pillow — **not needed** since CDN handles resizing.

## Implementation Plan

### Step 1: Add supabase-py dependency

```bash
uv add supabase
```

Add `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` to `.env.example` and `settings.py`.

### Step 2: Create the script

Create `scripts/upload_character_thumbnails.py` with:

1. **Load characters from DuckDB** — query all `character.id` where `is_likely_character = true` (1,444 characters)
2. **Batch fetch pageimage names** — call `pageimages` API with 50 titles per request (~29 API calls)
3. **Resolve manga variant** — for each character:
   - Take the `pageimage` filename
   - Replace `Anime` with `Manga` in the filename
   - Call `imageinfo` API to verify the manga file exists and get its `thumburl` at 150px width
   - Fall back to the anime `thumburl` if manga version doesn't exist
4. **Check Supabase Storage** — list existing files in the `character-images` bucket, skip characters that already have an uploaded image
5. **Download and upload** — for each new character:
   - Download the 150px thumbnail from the CDN `thumburl` (already webp, already resized)
   - Upload to Supabase Storage as `character-images/{character_id}.webp`
6. **Logging** — log progress, successes, failures, and characters where no image was found

### Step 3: Add CLI command and Makefile target

- Add a Click command in `cli/` (e.g. `upload-thumbnails`)
- Add `make upload-thumbnails` to Makefile

### Rate Limiting

- 1 second delay between individual API calls (imageinfo checks, downloads)
- Batch pageimage calls don't need extra delay (only ~29 calls total)
- Estimated total runtime: ~25-50 minutes

### Error Handling

- Retry failed downloads up to 3 times with backoff
- Log and skip characters with no image (don't fail the whole run)
- Script is idempotent — can be re-run safely (skips existing images)

### Supabase Bucket Setup (manual, one-time)

Create a `character-images` bucket in Supabase dashboard with public read access so the frontend can load images directly via URL:

```
https://<project>.supabase.co/storage/v1/object/public/character-images/{character_id}.webp
```
