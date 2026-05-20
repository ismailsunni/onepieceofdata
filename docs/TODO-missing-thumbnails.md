# TODO: Special handler for characters with no resolvable thumbnail

After adding the `prop=images` fallback in `scripts/upload_character_thumbnails.py`,
three characters still cannot be resolved through either `pageimages` or the
page's `prop=images` list:

- `Dr._Fishbonen`
- `Minatomo`
- `Ryuma`

## Why the existing fallback fails

For each of these pages, MediaWiki returns no `pageimage` *and* the `prop=images`
list does not include a usable infobox image (likely because the page is a
disambiguation/redirect, or the infobox image is transcluded from a template
rather than directly linked).

## What a special handler should do

Investigate per character and add a targeted strategy. Candidates:

1. **Follow redirects explicitly.** The character ID in DuckDB may point to a
   redirect page whose target uses a different name (e.g. `Ryuma` →
   `Shimotsuki_Ryuma`). Probe `action=query&redirects=1` and use the resolved
   title for the second pass.
2. **Parse the page wikitext for the infobox image field.** Use
   `action=parse&prop=wikitext` and extract `image = ...` from the infobox
   template. This is more invasive but works regardless of how the image is
   transcluded.
3. **Manual override map.** Keep a small `{character_id: filename}` dict in
   the script for the rare cases that need hand-curation.

## Acceptance

- `scripts/upload_character_thumbnails.py --dry-run` reports `0` characters
  with "no pageimage" / "no usable imageinfo" for the three listed IDs.
- Re-running the uploader successfully writes thumbnails for them to the
  `character-images` Supabase bucket.
