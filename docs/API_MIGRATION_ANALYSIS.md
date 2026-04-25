# Fandom API Migration Analysis

## Current Problem
The scrapers using `cloudscraper` are failing due to Cloudflare bot detection:
- `chapter.py` - Uses cloudscraper (FAILING)
- `character.py` - Uses cloudscraper (FAILING)
- `volume.py` - Uses cloudscraper (FAILING)
- `arc.py` - Uses urllib3 (May still work)
- `saga.py` - Uses urllib3 (May still work)

## Solution: Fandom MediaWiki API

### API Endpoints Available

The One Piece Fandom wiki runs on MediaWiki and provides a complete API at `https://onepiece.fandom.com/api.php`

#### 1. Parse API (action=parse)
Returns rendered HTML for a page:
```bash
curl "https://onepiece.fandom.com/api.php?action=parse&page=Chapter_1&format=json"
```

**Pros:**
- Similar to current HTML scraping approach
- Can reuse existing BeautifulSoup parsing logic

**Cons:**
- Still parsing HTML (brittle)
- Larger response sizes

#### 2. Query API (action=query)
Returns raw wikitext source:
```bash
curl "https://onepiece.fandom.com/api.php?action=query&titles=Chapter_1&prop=revisions&rvprop=content&format=json"
```

**Pros:**
- Structured wikitext with templates (e.g., `{{Chapter Box}}`)
- More stable and maintainable
- Smaller response sizes
- Can extract data directly from templates

**Cons:**
- Requires wikitext parsing
- Different parsing approach

## Recommended Approach: Query API with Wikitext Parsing

### Example Data Extraction

#### Chapter Data from Wikitext
```wikitext
{{Chapter Box
| title = Romance Dawn —The Dawn of the Adventure—
| jname = {{Ruby|ROMANCE DAWN|ロマンスドーン}}—冒険の夜明け—
| rname = Romansu Dōn -Bōken no Yoake-
| ename = Romance Dawn
}}
```

From this, we can extract:
- Title: "Romance Dawn —The Dawn of the Adventure—"
- Japanese name
- English name

#### Character List from Wikitext
```wikitext
{| class="CharTable"
!colspan="2"| [[Pirate]]s
|-
|
;[[Red Hair Pirates]]
*[[Shanks]]
*[[Benn Beckman]]
*[[Lucky Roux]]
```

Can extract character names and links directly.

### API Test Results

✅ **Basic Query**: Works without Cloudflare blocking
```bash
curl "https://onepiece.fandom.com/api.php?action=query&meta=siteinfo&format=json"
```

✅ **Parse Chapter**: Successfully retrieved Chapter 1 HTML
```bash
curl "https://onepiece.fandom.com/api.php?action=parse&page=Chapter_1&format=json"
```

✅ **Query Wikitext**: Successfully retrieved Chapter 1 raw wikitext
```bash
curl "https://onepiece.fandom.com/api.php?action=query&titles=Chapter_1&prop=revisions&rvprop=content&format=json"
```

## Implementation Plan

### Phase 1: Core API Client
Create a new `src/onepieceofdata/api/fandom_client.py`:
- Wrapper around MediaWiki API
- Handle API rate limiting
- Error handling and retries
- Parse wikitext templates

### Phase 2: Migrate Scrapers
Priority order (high impact first):

1. **Chapter Scraper** (CRITICAL - highest volume)
   - Use `action=query` to get wikitext
   - Parse `{{Chapter Box}}` template
   - Extract character table

2. **Character Scraper** (HIGH)
   - Use `action=query` to get character page wikitext
   - Parse infobox template
   - Extract structured data

3. **Volume Scraper** (MEDIUM)
   - Get volumes list page via API
   - Parse volume tables

### Phase 3: Testing
- Unit tests for API client
- Integration tests with real API
- Compare results with existing JSON data

### Phase 4: Deprecate cloudscraper
- Remove cloudscraper dependency
- Update all scrapers to use API

## Wikitext Parsing Strategy

### Option 1: Regex-based (Simple)
```python
import re

def parse_chapter_box(wikitext):
    match = re.search(r'\{\{Chapter Box\s*(.*?)\}\}', wikitext, re.DOTALL)
    if match:
        content = match.group(1)
        # Parse key-value pairs
        title_match = re.search(r'\|\s*title\s*=\s*(.+?)(?:\n|\|)', content)
        if title_match:
            return title_match.group(1).strip()
```

**Pros:** Simple, no dependencies
**Cons:** Can be brittle with nested templates

### Option 2: mwparserfromhell (Robust)
```python
import mwparserfromhell

wikicode = mwparserfromhell.parse(wikitext)
for template in wikicode.filter_templates():
    if template.name.matches("Chapter Box"):
        title = template.get("title").value.strip()
```

**Pros:** Robust, handles all wikitext edge cases
**Cons:** Additional dependency

**RECOMMENDED**: Use mwparserfromhell for robustness

## Benefits of Migration

1. **No More Cloudflare Issues**: API is designed for programmatic access
2. **More Stable**: Structured wikitext less likely to change than HTML
3. **Faster**: Smaller responses, can batch requests
4. **Official**: Using the official MediaWiki API
5. **Better Rate Limiting**: API provides clear rate limit headers
6. **Batch Operations**: Can query multiple pages in one request

## Risks & Mitigation

### Risk 1: API Rate Limiting
**Mitigation**:
- Respect rate limits (default: 50 requests/sec for anonymous users)
- Add delays between requests
- Use batch queries where possible

### Risk 2: Different Data Format
**Mitigation**:
- Create comprehensive test suite
- Compare API results with existing data
- Gradual migration (one scraper at a time)

### Risk 3: API Changes
**Mitigation**:
- MediaWiki API is very stable
- Version the API calls
- Add robust error handling

## Batch Query Example

Get multiple chapters in one request:
```bash
curl "https://onepiece.fandom.com/api.php?action=query&titles=Chapter_1|Chapter_2|Chapter_3&prop=revisions&rvprop=content&format=json"
```

This can significantly speed up scraping!

## Next Steps

1. Create API client with wikitext parsing
2. Build proof-of-concept for chapter scraper
3. Test against existing data
4. Migrate remaining scrapers
5. Remove cloudscraper dependency

## Code Structure

```
src/onepieceofdata/
├── api/
│   ├── __init__.py
│   ├── fandom_client.py      # MediaWiki API client
│   └── wikitext_parser.py    # Template parsing utilities
├── scrapers/
│   ├── chapter.py            # Migrate to API
│   ├── character.py          # Migrate to API
│   ├── volume.py             # Migrate to API
│   ├── arc.py                # Keep urllib3 or migrate
│   └── saga.py               # Keep urllib3 or migrate
```

## Dependencies to Add

```toml
dependencies = [
    "mwparserfromhell>=0.6",  # Wikitext parsing
    # ... existing dependencies
]
```

## Timeline Estimate

- Phase 1 (API Client): 2-3 hours
- Phase 2 (Migrate Scrapers): 4-6 hours
- Phase 3 (Testing): 2-3 hours
- Phase 4 (Cleanup): 1 hour

**Total**: ~10-13 hours of development work

## Conclusion

✅ **RECOMMENDED**: Migrate to Fandom MediaWiki API using wikitext parsing

The API is:
- Already accessible (tested successfully)
- Free from Cloudflare issues
- More stable and maintainable
- Officially supported

This migration will solve the current scraping issues and make the codebase more robust long-term.
