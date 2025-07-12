#!/usr/bin/env python3
"""Data quality checks and detailed comparisons."""

import duckdb
import pandas as pd

def data_quality_check(db_path: str, db_name: str):
    """Run data quality checks on a database."""
    print(f"\n🔍 Data Quality Check - {db_name}")
    print("=" * 40)
    
    conn = duckdb.connect(db_path)
    
    # Chapter quality checks
    print("📖 Chapter Data Quality:")
    try:
        # Check for missing chapter numbers
        missing_chapters = conn.execute("""
            SELECT COUNT(*) as missing_count 
            FROM chapter 
            WHERE number IS NULL OR number <= 0
        """).fetchone()[0]
        print(f"  Missing/invalid chapter numbers: {missing_chapters}")
        
        # Check chapter range
        chapter_range = conn.execute("""
            SELECT MIN(number) as min_chapter, MAX(number) as max_chapter, COUNT(*) as total
            FROM chapter
        """).fetchone()
        print(f"  Chapter range: {chapter_range[0]} - {chapter_range[1]} ({chapter_range[2]} total)")
        
        # Check for missing titles
        missing_titles = conn.execute("""
            SELECT COUNT(*) FROM chapter WHERE title IS NULL OR title = ''
        """).fetchone()[0]
        print(f"  Missing titles: {missing_titles}")
        
    except Exception as e:
        print(f"  Error checking chapters: {e}")
    
    # Character quality checks  
    print("\n👥 Character Data Quality:")
    try:
        char_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_characters,
                COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as with_names,
                COUNT(CASE WHEN bounty IS NOT NULL AND bounty > 0 THEN 1 END) as with_bounties,
                COUNT(CASE WHEN age IS NOT NULL AND age > 0 THEN 1 END) as with_ages
            FROM character
        """).fetchone()
        
        print(f"  Total characters: {char_stats[0]}")
        print(f"  With names: {char_stats[1]}")
        print(f"  With bounties: {char_stats[2]}")
        print(f"  With ages: {char_stats[3]}")
        
    except Exception as e:
        print(f"  Error checking characters: {e}")
    
    # CoC relationship checks
    print("\n🔗 Character-Chapter Relationships:")
    try:
        coc_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_relationships,
                COUNT(DISTINCT chapter) as chapters_with_chars,
                COUNT(DISTINCT character) as unique_characters
            FROM coc
        """).fetchone()
        
        print(f"  Total relationships: {coc_stats[0]}")
        print(f"  Chapters with characters: {coc_stats[1]}")
        print(f"  Unique character names: {coc_stats[2]}")
        
    except Exception as e:
        print(f"  Error checking CoC: {e}")
    
    conn.close()

def compare_specific_records(old_db: str, new_db: str):
    """Compare specific records that might be interesting."""
    print("\n🎯 Specific Record Comparisons")
    print("=" * 40)
    
    # Compare first chapter
    print("📖 Chapter 1 Comparison:")
    for db_path, name in [(old_db, "OLD"), (new_db, "NEW")]:
        conn = duckdb.connect(db_path)
        try:
            chapter1 = conn.execute("SELECT * FROM chapter WHERE number = 1").df()
            if not chapter1.empty:
                print(f"  {name}: {chapter1.iloc[0]['title']} (Vol {chapter1.iloc[0]['volume']}, {chapter1.iloc[0]['num_page']} pages)")
            else:
                print(f"  {name}: Chapter 1 not found")
        except Exception as e:
            print(f"  {name}: Error - {e}")
        conn.close()
    
    # Compare a specific character
    print("\n👤 Luffy Character Comparison:")
    for db_path, name in [(old_db, "OLD"), (new_db, "NEW")]:
        conn = duckdb.connect(db_path)
        try:
            luffy = conn.execute("SELECT * FROM character WHERE id LIKE '%Luffy%' OR name LIKE '%Luffy%' LIMIT 1").df()
            if not luffy.empty:
                row = luffy.iloc[0]
                print(f"  {name}: {row['name']} (Age: {row['age']}, Bounty: {row['bounty']})")
            else:
                print(f"  {name}: Luffy not found")
        except Exception as e:
            print(f"  {name}: Error - {e}")
        conn.close()

if __name__ == "__main__":
    old_db_path = "./data/onepiece-old.duckdb"
    new_db_path = "./data/onepiece.duckdb"
    
    data_quality_check(old_db_path, "OLD DATABASE")
    data_quality_check(new_db_path, "NEW DATABASE")
    compare_specific_records(old_db_path, new_db_path)
