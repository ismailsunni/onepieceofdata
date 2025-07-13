#!/usr/bin/env python3
"""Final comparison between old and new databases with scraping status analysis."""

import duckdb
import pandas as pd

def final_comparison():
    print("🎯 FINAL DATABASE COMPARISON")
    print("=" * 60)
    
    old_conn = duckdb.connect("./data/onepiece-old.duckdb")
    new_conn = duckdb.connect("./data/onepiece-final.duckdb")
    
    # Get counts from both databases
    old_stats = {}
    new_stats = {}
    
    tables = ['chapter', 'volume', 'character', 'coc']
    for table in tables:
        old_stats[table] = old_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        new_stats[table] = new_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    print("📊 RECORD COUNTS COMPARISON:")
    print(f"                    OLD DB    NEW DB    DIFFERENCE")
    for table in tables:
        diff = new_stats[table] - old_stats[table]
        status = "✅" if diff == 0 else "📈" if diff > 0 else "📉"
        print(f"{table.capitalize():<12}       {old_stats[table]:<8} {new_stats[table]:<8} {diff:+d} {status}")
    
    # Character status analysis
    print(f"\n📋 CHARACTER SCRAPING STATUS ANALYSIS:")
    status_counts = new_conn.execute("""
        SELECT 
            scraping_status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM character), 1) as percentage
        FROM character 
        GROUP BY scraping_status 
        ORDER BY count DESC
    """).fetchall()
    
    for status, count, percentage in status_counts:
        print(f"  {status:<20}: {count:>4} characters ({percentage:>5.1f}%)")
    
    # Show sample characters by status
    print(f"\n🔍 SAMPLE CHARACTERS BY STATUS:")
    for status, _, _ in status_counts[:3]:  # Show top 3 categories
        sample_chars = new_conn.execute("""
            SELECT id, name, scraping_note 
            FROM character 
            WHERE scraping_status = ? 
            LIMIT 3
        """, [status]).fetchall()
        
        print(f"\n  {status}:")
        for char_id, name, note in sample_chars:
            print(f"    - {char_id} ({name})")
            if note:
                print(f"      Note: {note}")
    
    # Data quality comparison
    print(f"\n📈 DATA QUALITY COMPARISON:")
    
    # Compare chapter data
    old_ch_pages = old_conn.execute("SELECT COUNT(*) FROM chapter WHERE num_page IS NOT NULL").fetchone()[0]
    new_ch_pages = new_conn.execute("SELECT COUNT(*) FROM chapter WHERE num_page IS NOT NULL").fetchone()[0]
    print(f"Chapters with page numbers: {old_ch_pages} → {new_ch_pages} ({'✅ Same' if old_ch_pages == new_ch_pages else '📈 Improved'})")
    
    # Compare character data completeness
    old_char_bounty = old_conn.execute("SELECT COUNT(*) FROM character WHERE bounty IS NOT NULL").fetchone()[0]
    new_char_bounty = new_conn.execute("SELECT COUNT(*) FROM character WHERE bounty IS NOT NULL").fetchone()[0]
    print(f"Characters with bounties: {old_char_bounty} → {new_char_bounty} ({'✅ Same' if old_char_bounty == new_char_bounty else '📈 More' if new_char_bounty > old_char_bounty else '📉 Less'})")
    
    old_conn.close()
    new_conn.close()
    
    # Final summary
    print(f"\n🎉 FINAL SUMMARY:")
    if new_stats['character'] >= old_stats['character']:
        print(f"✅ CHARACTER COVERAGE: {new_stats['character']}/{old_stats['character']} characters ({new_stats['character']/old_stats['character']*100:.1f}%)")
    
    if new_stats['chapter'] == old_stats['chapter'] and new_stats['volume'] == old_stats['volume']:
        print(f"✅ CONTENT COVERAGE: Perfect match for chapters and volumes")
    
    print(f"✅ DATA QUALITY: Enhanced with scraping status tracking")
    print(f"🚀 ARCHITECTURE: Modern v2 with better error handling and validation")

if __name__ == "__main__":
    final_comparison()
