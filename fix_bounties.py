#!/usr/bin/env python3
"""Fix bounty parsing issues in the database.

This script re-parses all bounty values from the bounties text field
and updates the numeric bounty field for characters that lost their
bounty values during previous processing.
"""

import duckdb
import re
from typing import Optional


def parse_bounty_fixed(bounties_raw: str) -> Optional[int]:
    """Fixed bounty parsing logic that handles the new bounty text format."""
    if not bounties_raw:
        return None

    # Handle both string and the semicolon format
    if isinstance(bounties_raw, str):
        # Split by semicolon to get parts
        parts = [part.strip() for part in bounties_raw.split(';')]
    else:
        parts = [str(bounties_raw)]

    # Look through all parts for numeric values
    for part in parts:
        # Clean up the part
        part = part.replace('¥', '').strip()
        
        # Skip obvious non-bounty parts
        if part in ['At least', 'Unknown', 'Over', '★', '(', ')'] or part.startswith('bounty'):
            continue
        if part.replace('★', '').replace(' ', '') == '':
            continue
            
        # Look for numbers with commas
        match = re.search(r'[\d,]+', part)
        if match:
            try:
                bounty = int(match.group().replace(',', ''))
                # Sanity check - bounties should be reasonable
                if bounty > 0 and bounty <= 10_000_000_000:  # Up to 10B berry
                    return bounty
            except ValueError:
                continue
    
    return None


def fix_bounties(db_path: str, dry_run: bool = False):
    """Fix bounty parsing issues in the database."""
    conn = duckdb.connect(db_path)
    
    try:
        # Find characters with missing bounty values but non-empty bounties text
        candidates = conn.execute("""
            SELECT id, name, bounties
            FROM character
            WHERE bounty IS NULL 
              AND bounties IS NOT NULL 
              AND bounties != ''
            ORDER BY name
        """).fetchall()
        
        print(f"🔍 Found {len(candidates)} characters with missing bounty values")
        
        fixed_count = 0
        total_bounty_restored = 0
        
        for char_id, name, bounties_text in candidates:
            # Try to parse the bounty
            parsed_bounty = parse_bounty_fixed(bounties_text)
            
            if parsed_bounty:
                print(f"✅ {name}: {parsed_bounty:,} berry")
                
                if not dry_run:
                    conn.execute("""
                        UPDATE character 
                        SET bounty = ?
                        WHERE id = ?
                    """, [parsed_bounty, char_id])
                
                fixed_count += 1
                total_bounty_restored += parsed_bounty
            else:
                print(f"❌ {name}: Could not parse bounty from '{bounties_text}'")
        
        print(f"\n📊 Results:")
        print(f"Characters fixed: {fixed_count}")
        print(f"Total bounty restored: {total_bounty_restored:,} berry")
        
        if dry_run:
            print("🔧 This was a dry run. Use --apply to make changes.")
        else:
            print("✅ Bounty values updated in database!")
            
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    
    db_path = "data/onepiece.duckdb"
    dry_run = "--apply" not in sys.argv
    
    if dry_run:
        print("🧪 DRY RUN MODE - No changes will be made")
        print("Use 'python fix_bounties.py --apply' to apply changes")
        print()
    
    fix_bounties(db_path, dry_run=dry_run)