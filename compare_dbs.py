#!/usr/bin/env python3
"""Compare old and new DuckDB databases."""

import duckdb
import pandas as pd
from pathlib import Path

def get_table_counts(db_path: str) -> dict:
    """Get row counts for all tables in a database."""
    conn = duckdb.connect(db_path)
    tables = ['chapter', 'volume', 'character', 'coc']
    counts = {}
    
    for table in tables:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = result[0] if result else 0
        except Exception as e:
            print(f"Error counting {table}: {e}")
            counts[table] = 0
    
    conn.close()
    return counts

def compare_databases(old_db: str, new_db: str):
    """Compare two databases and show differences."""
    print("🔍 Comparing Database Row Counts")
    print("=" * 50)
    
    old_counts = get_table_counts(old_db)
    new_counts = get_table_counts(new_db)
    
    print(f"{'Table':<12} {'Old DB':<10} {'New DB':<10} {'Difference':<12} {'Status'}")
    print("-" * 60)
    
    for table in ['volume', 'chapter', 'character', 'coc']:
        old_count = old_counts.get(table, 0)
        new_count = new_counts.get(table, 0)
        diff = new_count - old_count
        
        if diff == 0:
            status = "✅ Same"
        elif diff > 0:
            status = f"📈 +{diff}"
        else:
            status = f"📉 {diff}"
        
        print(f"{table:<12} {old_count:<10} {new_count:<10} {diff:<12} {status}")

def sample_data_comparison(old_db: str, new_db: str, table: str, limit: int = 5):
    """Compare sample data from a specific table."""
    print(f"\n📊 Sample Data Comparison - {table.upper()} Table")
    print("=" * 60)
    
    # Get sample from old DB
    old_conn = duckdb.connect(old_db)
    old_df = old_conn.execute(f"SELECT * FROM {table} ORDER BY 1 LIMIT {limit}").df()
    old_conn.close()
    
    # Get sample from new DB
    new_conn = duckdb.connect(new_db)
    new_df = new_conn.execute(f"SELECT * FROM {table} ORDER BY 1 LIMIT {limit}").df()
    new_conn.close()
    
    print("OLD DATABASE:")
    print(old_df.to_string(index=False))
    print("\nNEW DATABASE:")
    print(new_df.to_string(index=False))
    
    # Check if they're identical
    if old_df.equals(new_df):
        print("\n✅ Sample data is identical!")
    else:
        print("\n⚠️  Sample data differs!")

if __name__ == "__main__":
    # Update these paths to your actual database locations
    old_db_path = "./data/onepiece-old.duckdb"  # Your old one
    new_db_path = "./data/onepiece.duckdb"      # Your new one
    
    # Check if files exist
    if not Path(old_db_path).exists():
        print(f"❌ Old database not found: {old_db_path}")
        print("💡 Rename your old database to onepiece_old.duckdb")
        exit(1)
    
    if not Path(new_db_path).exists():
        print(f"❌ New database not found: {new_db_path}")
        print("💡 Run the parser first to create the new database")
        exit(1)
    
    # Compare databases
    compare_databases(old_db_path, new_db_path)
    
    # Sample data comparisons
    sample_data_comparison(old_db_path, new_db_path, "chapter", 3)
    sample_data_comparison(old_db_path, new_db_path, "character", 3)
    sample_data_comparison(old_db_path, new_db_path, "coc", 5)
