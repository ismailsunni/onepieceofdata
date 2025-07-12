#!/usr/bin/env python3
"""Compare database schemas and data types."""

import duckdb
import pandas as pd

def get_schema_info(db_path: str) -> dict:
    """Get schema information for all tables."""
    conn = duckdb.connect(db_path)
    schemas = {}
    
    tables = ['chapter', 'volume', 'character', 'coc']
    for table in tables:
        try:
            result = conn.execute(f"DESCRIBE {table}").df()
            schemas[table] = result
        except Exception as e:
            print(f"Error getting schema for {table}: {e}")
            schemas[table] = pd.DataFrame()
    
    conn.close()
    return schemas

def compare_schemas(old_db: str, new_db: str):
    """Compare schemas between two databases."""
    print("🏗️  Schema Comparison")
    print("=" * 50)
    
    old_schemas = get_schema_info(old_db)
    new_schemas = get_schema_info(new_db)
    
    for table in ['volume', 'chapter', 'character', 'coc']:
        print(f"\n📋 {table.upper()} Table Schema:")
        print("-" * 30)
        
        old_schema = old_schemas.get(table, pd.DataFrame())
        new_schema = new_schemas.get(table, pd.DataFrame())
        
        if old_schema.empty and new_schema.empty:
            print("❌ Both databases missing this table")
        elif old_schema.empty:
            print("❌ Old database missing this table")
        elif new_schema.empty:
            print("❌ New database missing this table")
        elif old_schema.equals(new_schema):
            print("✅ Schemas are identical")
        else:
            print("⚠️  Schemas differ:")
            print("OLD:")
            print(old_schema.to_string(index=False))
            print("NEW:")
            print(new_schema.to_string(index=False))

if __name__ == "__main__":
    old_db_path = "./data/onepiece-old.duckdb"
    new_db_path = "./data/onepiece.duckdb"
    
    compare_schemas(old_db_path, new_db_path)
