#!/usr/bin/env python3
"""Quick test to demonstrate API works while cloudscraper fails."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

print("=" * 70)
print("Testing: Fandom API vs Cloudscraper")
print("=" * 70)

# Test 1: Try with API
print("\n[TEST 1] Using Fandom API...")
try:
    from onepieceofdata.api import FandomAPIClient
    client = FandomAPIClient()
    wikitext = client.get_page_wikitext("Chapter 1")
    if wikitext and len(wikitext) > 0:
        print(f"✅ SUCCESS: Retrieved {len(wikitext)} characters")
        print(f"   Preview: {wikitext[:100]}...")
    else:
        print("❌ FAILED: No content retrieved")
except Exception as e:
    print(f"❌ FAILED: {str(e)}")

# Test 2: Try with cloudscraper (will likely fail)
print("\n[TEST 2] Using cloudscraper...")
try:
    import cloudscraper
    scraper = cloudscraper.create_scraper()
    response = scraper.get("https://onepiece.fandom.com/wiki/Chapter_1", timeout=10)
    if response.status_code == 200:
        print(f"✅ SUCCESS: Got HTTP {response.status_code}")
        print(f"   Content length: {len(response.content)}")
    else:
        print(f"❌ FAILED: HTTP {response.status_code}")
except Exception as e:
    print(f"❌ FAILED: {str(e)}")

print("\n" + "=" * 70)
print("Conclusion:")
print("=" * 70)
print("If API succeeded and cloudscraper failed, migration is necessary!")
print("The API approach bypasses Cloudflare completely.")
