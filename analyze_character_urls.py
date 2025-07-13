#!/usr/bin/env python3
"""Analyze and potentially fix character URL issues."""

import csv
import json
from typing import Dict, List, Set

def analyze_character_url_issues():
    print("🔍 CHARACTER URL ISSUE ANALYSIS")
    print("=" * 60)
    
    # Read the characters CSV
    characters = []
    with open('data/characters.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        characters = list(reader)
    
    print(f"📊 Total characters in CSV: {len(characters)}")
    
    # Categorize URL issues
    no_url = []
    external_url = []
    valid_url = []
    
    for char in characters:
        char_id = char['id']
        char_name = char['name']
        char_url = char['url']
        
        if not char_url or char_url.strip() == '':
            no_url.append((char_id, char_name))
        elif 'onepiece.fandom.com' not in char_url and '/wiki/' not in char_url:
            external_url.append((char_id, char_name, char_url))
        else:
            valid_url.append((char_id, char_name, char_url))
    
    print(f"\n📋 URL Category Breakdown:")
    print(f"  ✅ Valid URLs: {len(valid_url)}")
    print(f"  ❌ Missing URLs: {len(no_url)}")
    print(f"  🔗 External URLs: {len(external_url)}")
    
    print(f"\n❌ Characters with missing URLs ({len(no_url)}):")
    for char_id, char_name in no_url:
        print(f"  - {char_id} ({char_name})")
    
    print(f"\n🔗 Characters with external URLs ({len(external_url)}):")
    for char_id, char_name, char_url in external_url:
        print(f"  - {char_id} ({char_name}) → {char_url}")
    
    return no_url, external_url

def suggest_character_url_fixes(no_url_chars, external_url_chars):
    print(f"\n\n💡 CHARACTER URL FIX SUGGESTIONS")
    print("=" * 60)
    
    # For characters without URLs, suggest based on their names
    print("🔧 Suggested fixes for missing URLs:")
    for char_id, char_name in no_url_chars:
        # Generate potential wiki URL based on character name/ID
        suggested_url = f"/wiki/{char_id}"
        print(f"  {char_id}: {suggested_url}")
    
    print(f"\n🔧 Suggested fixes for external URLs:")
    for char_id, char_name, char_url in external_url_chars:
        if 'wikipedia.org' in char_url:
            # Try to convert Wikipedia URL to One Piece wiki URL
            suggested_url = f"/wiki/{char_id}"
            print(f"  {char_id}: {char_url} → {suggested_url}")

def check_scraped_character_success_rate():
    print(f"\n\n📊 CHARACTER SCRAPING SUCCESS RATE")
    print("=" * 60)
    
    # Load the detailed character JSON
    with open('data/characters_detail.json', 'r', encoding='utf-8') as f:
        scraped_characters = json.load(f)
    
    # Count successful vs failed scrapes
    successful_scrapes = []
    failed_scrapes = []
    
    for char in scraped_characters:
        char_id = char.get('id')
        # If character has meaningful data beyond just ID, consider it successful
        if len(char.keys()) > 1:  # More than just 'id'
            successful_scrapes.append(char_id)
        else:
            failed_scrapes.append(char_id)
    
    print(f"✅ Successful scrapes: {len(successful_scrapes)}")
    print(f"❌ Failed scrapes: {len(failed_scrapes)}")
    print(f"📊 Success rate: {len(successful_scrapes) / len(scraped_characters) * 100:.1f}%")
    
    if failed_scrapes:
        print(f"\n❌ Characters that failed to scrape:")
        for char_id in failed_scrapes[:10]:  # Show first 10
            print(f"  - {char_id}")
        if len(failed_scrapes) > 10:
            print(f"  ... and {len(failed_scrapes) - 10} more")
    
    return successful_scrapes, failed_scrapes

def compare_with_old_database_characters():
    print(f"\n\n🔍 COMPARISON WITH OLD DATABASE")
    print("=" * 60)
    
    # Get missing characters from our previous analysis
    missing_chars = [
        'Arashi', 'Hidélie_Penguin', 'Ichika', 'Kaio_Dragon', 'Nika',
        'Ogre_Sazae_Kancho', 'Sanka', 'Silver_Bat', 'Smiley_Risu',
        'Wandering_Kung_Fu_Dugong', 'Yonka', 'Yonka_2', 'Yonka_Two'
    ]
    
    # Check CSV for these characters
    with open('data/characters.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        csv_characters = {row['id']: row for row in reader}
    
    print(f"📋 Status of missing characters in CSV:")
    for char_id in missing_chars:
        if char_id in csv_characters:
            char_data = csv_characters[char_id]
            print(f"  ✅ {char_id}: Found in CSV")
            print(f"      URL: '{char_data['url']}'")
            if not char_data['url'] or 'wikipedia' in char_data['url']:
                print(f"      ⚠️  URL issue detected")
        else:
            print(f"  ❌ {char_id}: NOT found in CSV")

if __name__ == "__main__":
    # Analyze URL issues
    no_url_chars, external_url_chars = analyze_character_url_issues()
    
    # Suggest fixes
    suggest_character_url_fixes(no_url_chars, external_url_chars)
    
    # Check scraping success rate
    successful_scrapes, failed_scrapes = check_scraped_character_success_rate()
    
    # Compare with old database
    compare_with_old_database_characters()
