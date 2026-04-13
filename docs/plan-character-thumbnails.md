# Plan: Character Thumbnail Images

Download character images from the One Piece Wiki and upload to Supabase Storage as thumbnails for the React frontend at onepieceofdata.com.

## Status: Implemented

Script: `scripts/upload_character_thumbnails.py`
Makefile: `make upload-thumbnails`

## Decisions

- **Image size**: 300px width (character page infobox, like the wiki)
- **Image format**: webp (CDN returns this natively, no conversion needed)
- **Storage**: Direct upload to Supabase Storage (no Git LFS, no local cache)
- **No new dependencies**: Uses stdlib `urllib` for Supabase Storage REST API
- **No Pillow**: CDN handles server-side resizing

## Investigation Findings

### Image API

The Fandom MediaWiki `pageimages` API is the entry point:

```
https://onepiece.fandom.com/api.php?action=query&titles=Monkey_D._Luffy&prop=pageimages&format=json
```

- Returns the default infobox image filename (e.g. `Monkey_D._Luffy_Anime_Post_Timeskip_Infobox.png`)
- Supports `&redirects` to handle alias pages (e.g. Klahadore -> Kuro)
- Supports batching up to 50 titles per request (~29 calls for all 1,444 characters)

### Manga vs Anime Image Selection

The wiki infobox naming convention is consistent:

- `{Name}_Anime_Post_Timeskip_Infobox.png` -> `{Name}_Manga_Post_Timeskip_Infobox.png`
- `{Name}_Anime_Infobox.png` -> `{Name}_Manga_Infobox.png`

Strategy: Get `pageimage` (always returns anime version), replace `Anime` with `Manga` in the filename, verify it exists via `imageinfo` API. Fall back to anime if manga version doesn't exist.

Availability (tested across 90 random characters):

- 100% have a `pageimage`
- ~97% follow the `Anime`/`Manga` naming pattern
- 100% of those with the pattern have a manga version available
- ~3% have non-standard names (e.g. `Goa_Kingdom_Infobox.png`) -- used as-is

### Size Estimates (at 300px width)

| Metric | Value |
|--------|-------|
| Average per image | ~60 KB |
| Total (1,444 chars) | ~85 MB |
| Supabase free tier | 1 GB |

### Character ID Mapping

Character IDs in DuckDB (e.g. `Monkey_D._Luffy`) map directly to wiki page titles. The API normalizes underscores automatically.

## How It Works

1. Load character IDs from DuckDB (`is_likely_character = true`)
2. Batch fetch `pageimage` names from Fandom API (50 per request)
3. For each character:
   - Try manga variant (replace `Anime` with `Manga` in filename)
   - Get 300px thumbnail URL via `imageinfo` API
   - Fall back to anime/original if manga doesn't exist
4. Check Supabase Storage for existing images (skip unless `--force`)
5. Download thumbnail from CDN and upload to Supabase Storage
6. Log progress, successes, failures

## Usage

```bash
# Upload all character thumbnails
make upload-thumbnails

# Force re-upload all (overwrite existing)
uv run python scripts/upload_character_thumbnails.py --force

# Test with a small batch
uv run python scripts/upload_character_thumbnails.py --limit 10
```

## Prerequisites

1. Set `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` in `.env`
2. Create a `character-images` bucket in Supabase dashboard with public read access

Frontend URL pattern:
```
https://<project>.supabase.co/storage/v1/object/public/character-images/{character_id}.webp
```

## Rate Limiting

- 1 second delay between individual API calls
- Batch pageimage calls use minimal delay (~29 calls total)
- Estimated total runtime: ~25-50 minutes
