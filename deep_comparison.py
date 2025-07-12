#!/usr/bin/env python3
"""Deep content comparison between old and new databases."""

import duckdb
import pandas as pd
from typing import Dict, List, Tuple

def deep_chapter_comparison(old_db: str, new_db: str):
    """Compare every single chapter field in detail."""
    print("🔍 DEEP CHAPTER COMPARISON")
    print("=" * 60)
    
    old_conn = duckdb.connect(old_db)
    new_conn = duckdb.connect(new_db)
    
    # Get all chapters from both databases
    old_chapters = old_conn.execute("SELECT * FROM chapter ORDER BY number").df()
    new_chapters = new_conn.execute("SELECT * FROM chapter ORDER BY number").df()
    
    print(f"📊 Chapter counts: Old={len(old_chapters)}, New={len(new_chapters)}")
    
    # Check if we have the same chapters
    old_numbers = set(old_chapters['number'])
    new_numbers = set(new_chapters['number'])
    
    missing_in_new = old_numbers - new_numbers
    missing_in_old = new_numbers - old_numbers
    
    if missing_in_new:
        print(f"❌ Chapters missing in NEW: {sorted(missing_in_new)}")
    if missing_in_old:
        print(f"✅ New chapters in NEW: {sorted(missing_in_old)}")
    
    # Compare common chapters field by field
    common_chapters = old_numbers & new_numbers
    print(f"\n🔍 Comparing {len(common_chapters)} common chapters...")
    
    differences = {
        'volume': [],
        'title': [],
        'num_page': [],
        'date': [],
        'jump': []
    }
    
    for chapter_num in sorted(common_chapters):
        old_row = old_chapters[old_chapters['number'] == chapter_num].iloc[0]
        new_row = new_chapters[new_chapters['number'] == chapter_num].iloc[0]
        
        # Compare each field
        for field in ['volume', 'title', 'num_page', 'date', 'jump']:
            old_val = old_row[field]
            new_val = new_row[field]
            
            # Handle NaN comparison
            if pd.isna(old_val) and pd.isna(new_val):
                continue
            elif pd.isna(old_val) or pd.isna(new_val) or old_val != new_val:
                differences[field].append({
                    'chapter': chapter_num,
                    'old': old_val,
                    'new': new_val
                })
    
    # Report differences
    total_diffs = sum(len(diffs) for diffs in differences.values())
    if total_diffs == 0:
        print("✅ ALL CHAPTER DATA IS IDENTICAL!")
    else:
        print(f"❌ Found {total_diffs} differences:")
        
        for field, diffs in differences.items():
            if diffs:
                print(f"\n📋 {field.upper()} differences ({len(diffs)}):")
                for diff in diffs[:10]:  # Show first 10
                    print(f"  Ch{diff['chapter']}: '{diff['old']}' → '{diff['new']}'")
                if len(diffs) > 10:
                    print(f"  ... and {len(diffs) - 10} more")
    
    old_conn.close()
    new_conn.close()
    
    return differences

def deep_volume_comparison(old_db: str, new_db: str):
    """Compare every single volume field in detail."""
    print("\n\n📚 DEEP VOLUME COMPARISON")
    print("=" * 60)
    
    old_conn = duckdb.connect(old_db)
    new_conn = duckdb.connect(new_db)
    
    # Get all volumes from both databases
    old_volumes = old_conn.execute("SELECT * FROM volume ORDER BY number").df()
    new_volumes = new_conn.execute("SELECT * FROM volume ORDER BY number").df()
    
    print(f"📊 Volume counts: Old={len(old_volumes)}, New={len(new_volumes)}")
    
    # Check if we have the same volumes
    old_numbers = set(old_volumes['number'])
    new_numbers = set(new_volumes['number'])
    
    missing_in_new = old_numbers - new_numbers
    missing_in_old = new_numbers - old_numbers
    
    if missing_in_new:
        print(f"❌ Volumes missing in NEW: {sorted(missing_in_new)}")
    if missing_in_old:
        print(f"✅ New volumes in NEW: {sorted(missing_in_old)}")
    
    # Compare common volumes
    common_volumes = old_numbers & new_numbers
    print(f"\n🔍 Comparing {len(common_volumes)} common volumes...")
    
    title_differences = []
    
    for volume_num in sorted(common_volumes):
        old_row = old_volumes[old_volumes['number'] == volume_num].iloc[0]
        new_row = new_volumes[new_volumes['number'] == volume_num].iloc[0]
        
        old_title = old_row['title']
        new_title = new_row['title']
        
        if old_title != new_title:
            title_differences.append({
                'volume': volume_num,
                'old': old_title,
                'new': new_title
            })
    
    if not title_differences:
        print("✅ ALL VOLUME DATA IS IDENTICAL!")
    else:
        print(f"❌ Found {len(title_differences)} title differences:")
        for diff in title_differences[:10]:
            print(f"  Vol{diff['volume']}: '{diff['old']}' → '{diff['new']}'")
        if len(title_differences) > 10:
            print(f"  ... and {len(title_differences) - 10} more")
    
    old_conn.close()
    new_conn.close()
    
    return title_differences

def analyze_missing_pages(old_db: str, new_db: str):
    """Find chapters with missing or different page numbers."""
    print("\n\n📄 PAGE NUMBER ANALYSIS")
    print("=" * 60)
    
    old_conn = duckdb.connect(old_db)
    new_conn = duckdb.connect(new_db)
    
    # Get page number statistics
    old_pages = old_conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN num_page IS NOT NULL THEN 1 END) as with_pages,
            COUNT(CASE WHEN num_page IS NULL THEN 1 END) as missing_pages,
            MIN(num_page) as min_pages,
            MAX(num_page) as max_pages,
            AVG(num_page) as avg_pages
        FROM chapter
    """).fetchone()
    
    new_pages = new_conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN num_page IS NOT NULL THEN 1 END) as with_pages,
            COUNT(CASE WHEN num_page IS NULL THEN 1 END) as missing_pages,
            MIN(num_page) as min_pages,
            MAX(num_page) as max_pages,
            AVG(num_page) as avg_pages
        FROM chapter
    """).fetchone()
    
    print("📊 Page Number Statistics:")
    print(f"                    OLD DB    NEW DB    DIFF")
    print(f"Total chapters:     {old_pages[0]:<8} {new_pages[0]:<8} {new_pages[0] - old_pages[0]}")
    print(f"With page numbers:  {old_pages[1]:<8} {new_pages[1]:<8} {new_pages[1] - old_pages[1]}")
    print(f"Missing pages:      {old_pages[2]:<8} {new_pages[2]:<8} {new_pages[2] - old_pages[2]}")
    print(f"Min pages:          {old_pages[3]:<8} {new_pages[3]:<8}")
    print(f"Max pages:          {old_pages[4]:<8} {new_pages[4]:<8}")
    print(f"Avg pages:          {old_pages[5]:<8.1f} {new_pages[5]:<8.1f}")
    
    # Find specific chapters with page number issues
    old_missing = old_conn.execute("""
        SELECT number, title FROM chapter WHERE num_page IS NULL ORDER BY number
    """).fetchall()
    
    new_missing = new_conn.execute("""
        SELECT number, title FROM chapter WHERE num_page IS NULL ORDER BY number
    """).fetchall()
    
    print(f"\n📝 Chapters missing page numbers:")
    print(f"OLD DB: {len(old_missing)} chapters")
    if old_missing:
        for ch in old_missing[:5]:
            print(f"  Chapter {ch[0]}: {ch[1]}")
        if len(old_missing) > 5:
            print(f"  ... and {len(old_missing) - 5} more")
    
    print(f"NEW DB: {len(new_missing)} chapters")
    if new_missing:
        for ch in new_missing[:5]:
            print(f"  Chapter {ch[0]}: {ch[1]}")
        if len(new_missing) > 5:
            print(f"  ... and {len(new_missing) - 5} more")
    
    old_conn.close()
    new_conn.close()

def sample_raw_data_check(old_db: str, new_db: str):
    """Check raw data for specific chapters to see parsing differences."""
    print("\n\n🔬 SAMPLE RAW DATA CHECK")
    print("=" * 60)
    
    old_conn = duckdb.connect(old_db)
    new_conn = duckdb.connect(new_db)
    
    # Check specific chapters that might have issues
    test_chapters = [1, 100, 500, 1000, 1153]  # Sample across the range
    
    for chapter_num in test_chapters:
        print(f"\n📖 Chapter {chapter_num}:")
        
        # Get from old DB
        old_data = old_conn.execute("""
            SELECT number, volume, title, num_page, date, jump 
            FROM chapter WHERE number = ?
        """, [chapter_num]).fetchone()
        
        # Get from new DB
        new_data = new_conn.execute("""
            SELECT number, volume, title, num_page, date, jump 
            FROM chapter WHERE number = ?
        """, [chapter_num]).fetchone()
        
        if old_data and new_data:
            fields = ['number', 'volume', 'title', 'num_page', 'date', 'jump']
            all_match = True
            
            for i, field in enumerate(fields):
                old_val = old_data[i]
                new_val = new_data[i]
                
                if old_val != new_val:
                    print(f"  ❌ {field}: '{old_val}' → '{new_val}'")
                    all_match = False
            
            if all_match:
                print(f"  ✅ All fields match")
        elif old_data and not new_data:
            print(f"  ❌ Missing in NEW DB")
        elif not old_data and new_data:
            print(f"  ✅ New in NEW DB")
        else:
            print(f"  ❓ Missing in both DBs")
    
    old_conn.close()
    new_conn.close()

if __name__ == "__main__":
    old_db_path = "./data/onepiece-old.duckdb"
    new_db_path = "./data/onepiece-fixed.duckdb"
    
    # Run deep comparisons
    chapter_diffs = deep_chapter_comparison(old_db_path, new_db_path)
    volume_diffs = deep_volume_comparison(old_db_path, new_db_path)
    
    # Analyze page number issues specifically
    analyze_missing_pages(old_db_path, new_db_path)
    
    # Check sample raw data
    sample_raw_data_check(old_db_path, new_db_path)
    
    # Summary
    print("\n\n📋 SUMMARY")
    print("=" * 40)
    total_chapter_issues = sum(len(diffs) for diffs in chapter_diffs.values())
    if total_chapter_issues == 0:
        print("✅ Chapter data is identical")
    else:
        print(f"❌ {total_chapter_issues} chapter field differences found")
        
    if not volume_diffs:
        print("✅ Volume data is identical")
    else:
        print(f"❌ {len(volume_diffs)} volume differences found")
