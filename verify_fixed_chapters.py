#!/usr/bin/env python3
"""Verify the specific chapters that had page number issues."""

import duckdb

def verify_fixed_chapters():
    print("🔍 VERIFYING PREVIOUSLY PROBLEMATIC CHAPTERS")
    print("=" * 50)
    
    conn = duckdb.connect("./data/onepiece-fixed.duckdb")
    
    problem_chapters = [12, 999, 1024, 1145]
    
    for chapter_num in problem_chapters:
        result = conn.execute("""
            SELECT number, title, num_page 
            FROM chapter 
            WHERE number = ?
        """, [chapter_num]).fetchone()
        
        if result:
            print(f"📖 Chapter {result[0]}: '{result[1]}' → {result[2]} pages")
        else:
            print(f"❌ Chapter {chapter_num}: Not found!")
    
    conn.close()

if __name__ == "__main__":
    verify_fixed_chapters()
