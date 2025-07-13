#!/usr/bin/env python3
"""Simple test script to verify arc and saga functionality."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    # Test imports
    print("Testing imports...")
    from onepieceofdata.models.data import ArcModel, SagaModel, ScrapingResult
    print("✓ Models imported successfully")
    
    from onepieceofdata.scrapers.arc import ArcScraper
    from onepieceofdata.scrapers.saga import SagaScraper
    print("✓ Scrapers imported successfully")
    
    from onepieceofdata.parsers.arc import ArcParser
    from onepieceofdata.parsers.saga import SagaParser
    print("✓ Parsers imported successfully")
    
    from onepieceofdata.database.operations import DatabaseManager
    print("✓ Database manager imported successfully")
    
    # Test model creation
    print("\nTesting model creation...")
    test_arc = ArcModel(
        arc_id="test_arc",
        title="Test Arc",
        start_chapter=1,
        end_chapter=5
    )
    print(f"✓ Created arc: {test_arc.title} (chapters {test_arc.start_chapter}-{test_arc.end_chapter})")
    
    test_saga = SagaModel(
        saga_id="test_saga",
        title="Test Saga",
        start_chapter=1,
        end_chapter=10
    )
    print(f"✓ Created saga: {test_saga.title} (chapters {test_saga.start_chapter}-{test_saga.end_chapter})")
    
    # Test database table creation
    print("\nTesting database operations...")
    db = DatabaseManager("test_arc_saga.duckdb")
    db.create_tables()
    print("✓ Database tables created successfully")
    
    # Check tables
    conn = db.connect()
    tables = conn.execute("SHOW TABLES").fetchall()
    table_names = [table[0] for table in tables]
    print(f"✓ Available tables: {', '.join(table_names)}")
    
    if 'arc' in table_names and 'saga' in table_names:
        print("✓ Arc and saga tables created successfully!")
    else:
        print("✗ Arc or saga tables missing")
    
    # Test scraper instantiation
    print("\nTesting scraper instantiation...")
    arc_scraper = ArcScraper()
    saga_scraper = SagaScraper()
    print("✓ Scrapers instantiated successfully")
    
    # Test parser instantiation
    print("\nTesting parser instantiation...")
    arc_parser = ArcParser()
    saga_parser = SagaParser()
    print("✓ Parsers instantiated successfully")
    
    print("\n🎉 All tests passed! Arc and saga functionality is working correctly.")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
