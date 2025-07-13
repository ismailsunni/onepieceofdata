#!/usr/bin/env python3
"""Compare character scraping between old and new approaches."""

import json
import duckdb
from collections import defaultdict

def analyze_character_differences():
    print("🔍 CHARACTER SCRAPING ANALYSIS")
    print("=" * 60)
    
    # Load both databases
    old_conn = duckdb.connect("./data/onepiece-old.duckdb")
    new_conn = duckdb.connect("./data/onepiece-fixed.duckdb")
    
    # Get all character IDs from both databases
    old_chars = old_conn.execute("SELECT id, name FROM character ORDER BY id").fetchall()
    new_chars = new_conn.execute("SELECT id, name FROM character ORDER BY id").fetchall()
    
    old_char_ids = {char[0] for char in old_chars}
    new_char_ids = {char[0] for char in new_chars}
    
    print(f"📊 Character counts: Old={len(old_char_ids)}, New={len(new_char_ids)}")
    
    # Find differences
    missing_in_new = old_char_ids - new_char_ids
    new_in_new = new_char_ids - old_char_ids
    
    print(f"\n❌ Missing in NEW ({len(missing_in_new)}):")
    old_char_dict = {char[0]: char[1] for char in old_chars}
    for char_id in sorted(missing_in_new):
        name = old_char_dict.get(char_id, 'Unknown')
        print(f"  - {char_id} ({name})")
    
    if new_in_new:
        print(f"\n✅ New in NEW ({len(new_in_new)}):")
        new_char_dict = {char[0]: char[1] for char in new_chars}
        for char_id in sorted(new_in_new):
            name = new_char_dict.get(char_id, 'Unknown')
            print(f"  - {char_id} ({name})")
    
    old_conn.close()
    new_conn.close()
    
    return missing_in_new

def analyze_raw_character_data():
    print("\n\n📋 RAW CHARACTER DATA ANALYSIS")
    print("=" * 60)
    
    # Load the new scraped character data
    with open('data/characters_detail.json', 'r', encoding='utf-8') as f:
        new_characters = json.load(f)
    
    print(f"📄 JSON file contains {len(new_characters)} characters")
    
    # Group by potential URL patterns to find shared pages
    url_groups = defaultdict(list)
    id_patterns = defaultdict(list)
    
    for char in new_characters:
        char_id = char.get('id', '')
        url = char.get('url', '')
        
        # Look for patterns that might indicate shared pages
        if url:
            base_url = url.split('#')[0]  # Remove anchors
            url_groups[base_url].append(char_id)
        
        # Look for ID patterns
        if '_' in char_id:
            base_id = char_id.split('_')[0]
            id_patterns[base_id].append(char_id)
    
    # Find pages with multiple characters
    print(f"\n🔗 Pages with multiple characters:")
    multi_char_pages = {url: chars for url, chars in url_groups.items() if len(chars) > 1}
    
    for url, chars in sorted(multi_char_pages.items()):
        print(f"  {url}: {len(chars)} characters")
        for char in chars[:5]:  # Show first 5
            print(f"    - {char}")
        if len(chars) > 5:
            print(f"    ... and {len(chars) - 5} more")
    
    print(f"\n📊 Found {len(multi_char_pages)} pages with multiple characters")
    
    return new_characters

def check_specific_missing_characters(missing_chars):
    print(f"\n\n🔎 CHECKING SPECIFIC MISSING CHARACTERS")
    print("=" * 60)
    
    # Load the new character data
    with open('data/characters_detail.json', 'r', encoding='utf-8') as f:
        new_characters = json.load(f)
    
    new_char_dict = {char.get('id'): char for char in new_characters}
    
    # Check if missing characters exist with different IDs
    for missing_id in sorted(missing_chars):
        print(f"\n🔍 Looking for '{missing_id}':")
        
        # Direct match
        if missing_id in new_char_dict:
            print(f"  ✅ Found exact match")
            continue
        
        # Look for similar names or variations
        found_variations = []
        missing_name = missing_id.replace('_', ' ')
        
        for char in new_characters:
            char_id = char.get('id', '')
            char_name = char.get('name', '')
            
            # Check for name variations
            if (char_name and char_id and 
                (missing_name.lower() in char_name.lower() or 
                 char_name.lower() in missing_name.lower() or
                 missing_id.lower() in char_id.lower())):
                found_variations.append((char_id, char_name))
        
        if found_variations:
            print(f"  🔄 Possible variations found:")
            for var_id, var_name in found_variations[:3]:
                print(f"    - {var_id} ({var_name})")
        else:
            print(f"  ❌ No variations found - likely missing from scrape")

def compare_scraping_approaches():
    print(f"\n\n⚖️  SCRAPING APPROACH COMPARISON")
    print("=" * 60)
    
    # Check if we have access to old scraping logic
    try:
        # Look for old scraper files
        import os
        old_scraper_files = []
        for root, dirs, files in os.walk('.'):
            for file in files:
                if 'character' in file.lower() and file.endswith('.py'):
                    old_scraper_files.append(os.path.join(root, file))
        
        print(f"📁 Found potential scraper files:")
        for file in old_scraper_files:
            print(f"  - {file}")
            
    except Exception as e:
        print(f"❌ Could not analyze old scraper files: {e}")

if __name__ == "__main__":
    missing_chars = analyze_character_differences()
    new_characters = analyze_raw_character_data()
    check_specific_missing_characters(missing_chars)
    compare_scraping_approaches()
