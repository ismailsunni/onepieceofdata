#!/usr/bin/env python3
"""Find missing characters between databases."""

import duckdb

def find_missing_characters():
    """Find characters that exist in old DB but not in new DB."""
    old_conn = duckdb.connect("./data/onepiece-old.duckdb")
    new_conn = duckdb.connect("./data/onepiece.duckdb")
    
    # Get character IDs from both databases
    old_chars = set([row[0] for row in old_conn.execute("SELECT id FROM character").fetchall()])
    new_chars = set([row[0] for row in new_conn.execute("SELECT id FROM character").fetchall()])
    
    missing_in_new = old_chars - new_chars
    missing_in_old = new_chars - old_chars
    
    print(f"📊 Character Analysis:")
    print(f"Old DB characters: {len(old_chars)}")
    print(f"New DB characters: {len(new_chars)}")
    print(f"Missing in new DB: {len(missing_in_new)}")
    print(f"Missing in old DB: {len(missing_in_old)}")
    
    if missing_in_new:
        print(f"\n❌ Characters missing in NEW database:")
        for char_id in sorted(list(missing_in_new)[:10]):  # Show first 10
            char_info = old_conn.execute("SELECT id, name FROM character WHERE id = ?", [char_id]).fetchone()
            print(f"  - {char_info[0]}: {char_info[1]}")
        if len(missing_in_new) > 10:
            print(f"  ... and {len(missing_in_new) - 10} more")
    
    if missing_in_old:
        print(f"\n✅ New characters in NEW database:")
        for char_id in sorted(list(missing_in_old)[:10]):  # Show first 10
            char_info = new_conn.execute("SELECT id, name FROM character WHERE id = ?", [char_id]).fetchone()
            print(f"  - {char_info[0]}: {char_info[1]}")
        if len(missing_in_old) > 10:
            print(f"  ... and {len(missing_in_old) - 10} more")
    
    old_conn.close()
    new_conn.close()

if __name__ == "__main__":
    find_missing_characters()
