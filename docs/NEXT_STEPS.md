# Next Steps: Migrating to Fandom API

## ✅ Proof of Concept Complete

The POC successfully demonstrated:
- ✅ API access works without Cloudflare blocking
- ✅ Chapter data extraction from wikitext
- ✅ Character table parsing
- ✅ Batch querying (5 chapters in ~400ms)
- ✅ Found MORE characters than HTML scraping (23 vs 19)

## Quick Start

Run the proof of concept:
```bash
uv run python poc_api_scraper.py
```

## Implementation Files Created

### 1. Core API Infrastructure
- `src/onepieceofdata/api/fandom_client.py` - MediaWiki API client
  - Single page queries
  - Batch queries (up to 50 pages)
  - Rate limiting
  - Retry logic

- `src/onepieceofdata/api/wikitext_parser.py` - Wikitext parsing utilities
  - Template extraction
  - Parameter parsing
  - Text cleaning
  - Chapter/character parsing

### 2. Documentation
- `API_MIGRATION_ANALYSIS.md` - Full analysis and migration plan
- `NEXT_STEPS.md` - This file

### 3. Proof of Concept
- `poc_api_scraper.py` - Working demonstration

## Migration Priority

### Phase 1: Chapter Scraper (HIGH PRIORITY)
**Why first?** Highest volume (1000+ chapters), most critical data

**Files to modify:**
- `src/onepieceofdata/scrapers/chapter.py`

**Changes needed:**
1. Replace `cloudscraper` with `FandomAPIClient`
2. Replace HTML parsing with wikitext parsing
3. Update `scrape_chapter()` method:
   ```python
   def scrape_chapter(self, chapter_number: int) -> ScrapingResult:
       client = FandomAPIClient()
       wikitext = client.get_page_wikitext(f"Chapter {chapter_number}")
       chapter_info = WikitextParser.parse_chapter_box(wikitext)
       characters = WikitextParser.parse_character_table(wikitext)
       # ...
   ```

4. Update parallel scraping to use batch queries
5. Test thoroughly

**Estimated time:** 3-4 hours

### Phase 2: Character Scraper
**Files to modify:**
- `src/onepieceofdata/scrapers/character.py`

**Changes needed:**
1. Replace `cloudscraper` with `FandomAPIClient`
2. Parse character infobox from wikitext
3. Add wikitext parser for character data

**Estimated time:** 2-3 hours

### Phase 3: Volume Scraper
**Files to modify:**
- `src/onepieceofdata/scrapers/volume.py`

**Changes needed:**
1. Get "Chapters_and_Volumes/Volumes" page via API
2. Parse volume tables from HTML or wikitext
3. Consider using `action=parse` for this one since it's a single page with tables

**Estimated time:** 2-3 hours

### Phase 4: Arc & Saga Scrapers (OPTIONAL)
**Current status:** Using `urllib3` - may still work

**Decision needed:**
- Test if current urllib3 approach still works
- If yes: keep as-is
- If no: migrate to API (similar to current approach)

### Phase 5: Cleanup
1. Remove `cloudscraper` from dependencies
2. Update tests
3. Update documentation
4. Archive old scrapers

**Estimated time:** 1-2 hours

## Decision Points

### 1. Wikitext Parsing Library

**Option A: Current regex-based approach (in POC)**
- ✅ No additional dependencies
- ✅ Works for simple templates
- ❌ May be brittle for complex nested templates

**Option B: Add mwparserfromhell**
- ✅ Robust, handles all wikitext edge cases
- ✅ Maintained library
- ❌ Additional dependency

**RECOMMENDATION:** Start with regex-based (Option A). If we encounter parsing issues during testing, switch to mwparserfromhell (Option B).

### 2. Migration Strategy

**Option A: Gradual (Recommended)**
- Migrate chapter scraper first
- Test thoroughly with existing data
- Migrate character scraper
- Test
- Continue...

**Option B: Big Bang**
- Migrate all scrapers at once
- Higher risk

**RECOMMENDATION:** Gradual migration (Option A)

### 3. Backward Compatibility

**Should we keep old scrapers?**
- Yes, as backup during migration
- Archive in `src/onepieceofdata/scrapers/legacy/`
- Remove after full migration is stable

## Testing Strategy

### Unit Tests
Create tests for new API components:
```python
# tests/test_fandom_client.py
def test_get_page_wikitext():
    client = FandomAPIClient()
    wikitext = client.get_page_wikitext("Chapter 1")
    assert wikitext is not None
    assert "Chapter Box" in wikitext

# tests/test_wikitext_parser.py
def test_parse_chapter_box():
    wikitext = "{{Chapter Box | title = Test }}"
    result = WikitextParser.parse_chapter_box(wikitext)
    assert result["title"] == "Test"
```

### Integration Tests
Compare API results with existing JSON data:
```python
def test_chapter_consistency():
    # Load existing data
    with open("data/chapters.json") as f:
        existing = json.load(f)

    # Scrape via API
    client = FandomAPIClient()
    for chapter in existing[:10]:  # Test first 10
        wikitext = client.get_page_wikitext(f"Chapter {chapter['chapter_number']}")
        info = WikitextParser.parse_chapter_box(wikitext)

        # Compare
        assert info["title"] in chapter["title"]
```

### Load Testing
- Test batch queries with 50+ chapters
- Verify rate limiting works
- Check memory usage

## Benefits Summary

### Immediate Benefits
1. **No Cloudflare blocking** - Main problem solved
2. **Faster scraping** - Batch queries reduce requests
3. **More accurate data** - POC found 4 more characters than HTML scraper
4. **Official API** - Designed for programmatic access

### Long-term Benefits
1. **More stable** - Less affected by HTML changes
2. **Better maintainability** - Cleaner parsing logic
3. **Easier debugging** - Structured wikitext easier to inspect
4. **API features** - Can use search, categories, etc.

## Potential Issues & Solutions

### Issue 1: API Rate Limiting
**Solution:**
- Current delay (1s) is fine
- API allows 50 requests/sec for anonymous
- Can create Fandom account for higher limits if needed

### Issue 2: Complex Template Parsing
**Solution:**
- Add `mwparserfromhell` dependency if regex isn't sufficient
- Most templates in POC parsed successfully

### Issue 3: Missing Data
**Solution:**
- Some pages may not have proper templates
- Add fallback to HTML parsing via `action=parse`
- Log warnings for manual review

### Issue 4: API Changes
**Solution:**
- MediaWiki API is very stable (10+ years)
- Version the API calls
- Add integration tests to catch changes early

## How to Start Migration

### Step 1: Set up environment
```bash
# Already have the POC working
# No additional setup needed for regex-based approach

# If choosing mwparserfromhell:
uv add mwparserfromhell
```

### Step 2: Create branch
```bash
git checkout -b feature/api-migration
```

### Step 3: Migrate chapter scraper
```bash
# Backup current implementation
cp src/onepieceofdata/scrapers/chapter.py src/onepieceofdata/scrapers/chapter.py.backup

# Modify chapter.py to use API
# ... implement changes ...

# Test
uv run pytest tests/test_chapter_scraper.py -v
```

### Step 4: Compare results
```bash
# Scrape a sample with old approach
# Scrape same sample with new approach
# Compare JSON outputs
```

### Step 5: Full test run
```bash
# Test with chapters 1-10
make test-scrape-parallel  # Old way
# vs
uv run python src/onepieceofdata/cli.py scrape-chapters --start 1 --end 10  # New way

# Compare data/chapters.json
```

### Step 6: Commit and continue
```bash
git add .
git commit -m "Migrate chapter scraper to Fandom API"

# Continue with character scraper...
```

## Success Metrics

Track these to measure migration success:
- ✅ Zero Cloudflare errors
- ✅ Scraping speed (should be faster with batch queries)
- ✅ Data completeness (compare with existing JSON)
- ✅ Error rate (should be lower)
- ✅ Character count accuracy (POC shows 21% more characters found)

## Questions?

Review these documents:
1. `API_MIGRATION_ANALYSIS.md` - Detailed technical analysis
2. `poc_api_scraper.py` - Working code example
3. MediaWiki API docs: https://www.mediawiki.org/wiki/API:Main_page

## Timeline

**Minimum (essential only):**
- Chapter scraper: 3-4 hours
- Character scraper: 2-3 hours
- Testing: 2 hours
- **Total: ~8 hours**

**Complete migration:**
- All scrapers: 8-10 hours
- Comprehensive testing: 3 hours
- Cleanup & docs: 1 hour
- **Total: ~13 hours**

## Conclusion

✅ **The Fandom API approach is proven and ready to implement**

The POC demonstrates that this migration will:
1. Solve the Cloudflare blocking issue
2. Improve scraping speed with batch queries
3. Find more accurate data (23 vs 19 characters)
4. Use a more maintainable approach

**Recommended action:** Start with Phase 1 (chapter scraper migration) and proceed gradually through the other scrapers.
