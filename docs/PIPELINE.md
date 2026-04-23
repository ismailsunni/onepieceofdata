# DuckDB to Supabase Export Pipeline - Implementation Plan

## Executive Summary

This document outlines a comprehensive plan to implement a database export pipeline that syncs the local DuckDB database (`onepiece.duckdb`) to Supabase (PostgreSQL). The implementation supports both manual CLI-based export and automatic GitHub Actions-triggered export when the database changes.

---

## Table of Contents

1. [Prerequisites and Setup](#prerequisites-and-setup)
2. [Architecture Overview](#architecture-overview)
3. [Implementation Phases](#implementation-phases)
4. [Technical Specifications](#technical-specifications)
5. [Security Considerations](#security-considerations)
6. [Testing Strategy](#testing-strategy)
7. [Challenges and Solutions](#challenges-and-solutions)

---

## Prerequisites and Setup

### 1. Supabase Setup

**Required Actions:**
1. Create a Supabase project at https://supabase.com
2. Obtain connection credentials:
   - Database URL (PostgreSQL connection string)
   - Service role key (for API access)
   - Project reference ID

**Database Configuration:**
```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema for One Piece data (optional, for organization)
CREATE SCHEMA IF NOT EXISTS onepiece;
```

### 2. Python Dependencies

**Core Libraries to Add:**
```toml
# Add to pyproject.toml [project.dependencies]
dependencies = [
    # ... existing dependencies ...
    "psycopg2-binary>=2.9.9",     # PostgreSQL adapter
    "sqlalchemy>=2.0.0",          # ORM and schema management
    "supabase>=2.0.0",            # Supabase Python client (optional)
]
```

**Alternative: Use existing dependencies only**
- Since you already have `duckdb` and `pandas`, you could use:
  - `duckdb` → export to Parquet/CSV
  - PostgreSQL COPY command to import
  - This is simpler but less flexible

**Recommended Approach:** Use SQLAlchemy for maximum flexibility and proper type mapping.

### 3. Environment Variables

**Add to `.env` file:**
```bash
# Supabase Configuration
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=your-service-role-key
SUPABASE_DB_URL=postgresql://postgres:password@db.xxxxx.supabase.co:5432/postgres

# Export Configuration
EXPORT_MODE=incremental  # or 'full'
EXPORT_BATCH_SIZE=1000
EXPORT_ON_PUSH=true
```

**Add to `src/onepieceofdata/config/settings.py`:**
```python
# Supabase Configuration
supabase_url: Optional[str] = Field(default=None, alias="SUPABASE_URL")
supabase_key: Optional[str] = Field(default=None, alias="SUPABASE_KEY")
supabase_db_url: Optional[str] = Field(default=None, alias="SUPABASE_DB_URL")

# Export Configuration
export_mode: str = Field(default="incremental", alias="EXPORT_MODE")
export_batch_size: int = Field(default=1000, alias="EXPORT_BATCH_SIZE")
export_on_push: bool = Field(default=True, alias="EXPORT_ON_PUSH")
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Local Development                        │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐  │
│  │   Scrapers   │ ───> │   DuckDB     │ ───> │    Export    │  │
│  │  (Chapters,  │      │  Database    │      │   Manager    │  │
│  │  Characters, │      │ (onepiece.   │      │              │  │
│  │   Volumes)   │      │  duckdb)     │      │              │  │
│  └──────────────┘      └──────────────┘      └──────┬───────┘  │
│                                                       │          │
└───────────────────────────────────────────────────────┼──────────┘
                                                        │
                                                        ▼
                                        ┌───────────────────────────┐
                                        │  Schema Transformer       │
                                        │  - Type mapping           │
                                        │  - Constraint handling    │
                                        │  - Metadata tracking      │
                                        └──────────┬────────────────┘
                                                   │
                                                   ▼
                          ┌────────────────────────────────────────┐
                          │         Supabase (PostgreSQL)          │
                          │  ┌──────────┐  ┌──────────┐            │
                          │  │  Tables  │  │Metadata  │            │
                          │  │ (7 core) │  │  Table   │            │
                          │  └──────────┘  └──────────┘            │
                          └────────────────────────────────────────┘
                                         ▲
                                         │
                          ┌──────────────┴──────────────┐
                          │   GitHub Actions (Auto)     │
                          │  Triggered on DB changes    │
                          └─────────────────────────────┘
```

### Key Components

1. **ExportManager**: Core class handling export logic
2. **SchemaMapper**: Handles DuckDB → PostgreSQL type mapping
3. **MetadataTracker**: Tracks sync state for incremental updates
4. **GitHub Actions Workflow**: Automated trigger on database changes

---

## Implementation Phases

### Phase 1: Core Export Infrastructure (Week 1)

**Files to Create:**
- `src/onepieceofdata/database/export.py` - Main export logic
- `src/onepieceofdata/database/schema_mapper.py` - Schema translation
- `src/onepieceofdata/database/metadata.py` - Sync state tracking

**Key Features:**
- Full database export (all tables)
- Schema creation in PostgreSQL
- Basic error handling
- Progress logging

**Deliverable:** Manual export working via CLI

### Phase 2: Incremental Updates (Week 2)

**Enhancements:**
- Add metadata tracking table
- Implement change detection
- Support upsert operations
- Handle deletions (if needed)

**Key Features:**
- Track last sync timestamp
- Compare row counts
- Identify changed records
- Batch processing for large datasets

**Deliverable:** Incremental sync working reliably

### Phase 3: CLI Integration (Week 2)

**File to Modify:**
- `src/onepieceofdata/cli.py` - Add export commands

**Commands to Add:**
```bash
# Full export
onepieceofdata export-supabase --mode full

# Incremental export
onepieceofdata export-supabase --mode incremental

# Dry run
onepieceofdata export-supabase --dry-run

# Specific tables
onepieceofdata export-supabase --tables chapter,volume,character

# Check sync status
onepieceofdata sync-status
```

**Deliverable:** Full CLI interface for manual exports

### Phase 4: GitHub Actions Automation (Week 3)

**Files to Create:**
- `.github/workflows/export-to-supabase.yml`
- `.github/scripts/check-db-changes.sh`

**Workflow Features:**
- Detect database file changes
- Run export on push to main/master
- Secure credential management
- Failure notifications

**Deliverable:** Automated export on push

### Phase 5: Testing & Documentation (Week 3-4)

**Testing:**
- Unit tests for export logic
- Integration tests with test database
- Schema validation tests
- Performance benchmarks

**Documentation:**
- Usage guide
- Troubleshooting guide
- Architecture documentation

**Deliverable:** Production-ready system

---

## Technical Specifications

### 1. Schema Mapping (DuckDB → PostgreSQL)

**Type Conversions:**
```python
DUCKDB_TO_POSTGRES_TYPES = {
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'VARCHAR': 'TEXT',
    'DATE': 'DATE',
    'BOOLEAN': 'BOOLEAN',
    'DOUBLE': 'DOUBLE PRECISION',
    'TIMESTAMP': 'TIMESTAMP',
}
```

**Key Considerations:**
1. **VARCHAR length**: DuckDB VARCHAR is unlimited; PostgreSQL defaults to unlimited TEXT
2. **Foreign Keys**: Need to create in correct order (volume → chapter → arc/saga)
3. **Indexes**: Add for performance (primary keys, foreign keys)
4. **Constraints**: Preserve NOT NULL, UNIQUE, CHECK constraints

### 2. Export Manager Architecture

**File: `src/onepieceofdata/database/export.py`**

```python
class SupabaseExportManager:
    """Manages exporting DuckDB data to Supabase PostgreSQL."""
    
    def __init__(self, db_manager: DatabaseManager, pg_connection_string: str):
        self.db = db_manager
        self.pg_url = pg_connection_string
        self.engine = create_engine(pg_connection_string)
        self.metadata_tracker = MetadataTracker(self.engine)
        
    def export_full(self, tables: Optional[List[str]] = None) -> ExportResult:
        """Full export of all or specified tables."""
        # 1. Create schema if not exists
        # 2. Create tables
        # 3. Export data in batches
        # 4. Verify counts
        # 5. Update metadata
        
    def export_incremental(self, tables: Optional[List[str]] = None) -> ExportResult:
        """Incremental export of changed data."""
        # 1. Check metadata for last sync
        # 2. Identify changes
        # 3. Upsert changed records
        # 4. Update metadata
        
    def verify_schema(self) -> SchemaValidationResult:
        """Verify PostgreSQL schema matches DuckDB."""
        
    def get_sync_status(self) -> SyncStatus:
        """Get current sync status and statistics."""
```

**File: `src/onepieceofdata/database/schema_mapper.py`**

```python
class SchemaMapper:
    """Maps DuckDB schema to PostgreSQL schema."""
    
    def create_postgres_schema(self, table_name: str) -> str:
        """Generate CREATE TABLE SQL for PostgreSQL."""
        
    def map_column_type(self, duckdb_type: str) -> str:
        """Map DuckDB type to PostgreSQL type."""
        
    def preserve_constraints(self, table_name: str) -> List[str]:
        """Extract and translate constraints."""
```

**File: `src/onepieceofdata/database/metadata.py`**

```python
class MetadataTracker:
    """Tracks sync state in PostgreSQL."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self._ensure_metadata_table()
        
    def _ensure_metadata_table(self):
        """Create sync_metadata table if not exists."""
        # Schema:
        # - table_name: VARCHAR
        # - last_sync_at: TIMESTAMP
        # - record_count: INTEGER
        # - checksum: VARCHAR (optional)
        
    def update_sync_metadata(self, table: str, count: int):
        """Update sync metadata after export."""
        
    def get_last_sync(self, table: str) -> Optional[datetime]:
        """Get last sync timestamp for a table."""
```

### 3. Incremental Update Strategy

**Approach: Full Table Replacement with Versioning**

Since DuckDB doesn't have built-in change tracking and our data isn't frequently updated:

1. **Simple Approach** (Recommended for initial implementation):
   - Compare row counts between DuckDB and PostgreSQL
   - If different, do full table refresh
   - Use TRUNCATE + INSERT for atomicity
   - Fast and reliable for medium-sized datasets

2. **Advanced Approach** (Optional for future):
   - Calculate hash/checksum of data
   - Detect which records changed
   - Use UPSERT (INSERT ... ON CONFLICT UPDATE)
   - More complex but efficient for large datasets

**Recommended: Start with Simple, upgrade if needed**

```python
def export_incremental(self, table_name: str):
    """Incremental export using row count comparison."""
    duckdb_count = self.db.query(f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0]
    pg_count = self.get_postgres_count(table_name)
    
    if duckdb_count != pg_count:
        logger.info(f"Table {table_name} changed: {pg_count} -> {duckdb_count}")
        self._full_table_refresh(table_name)
    else:
        logger.info(f"Table {table_name} unchanged, skipping")
```

### 4. GitHub Actions Workflow

**File: `.github/workflows/export-to-supabase.yml`**

```yaml
name: Export to Supabase

on:
  push:
    branches:
      - master
      - main
    paths:
      - 'data/onepiece.duckdb'
      - 'onepiece.duckdb'

  workflow_dispatch:  # Allow manual trigger
    inputs:
      mode:
        description: 'Export mode'
        required: true
        default: 'incremental'
        type: choice
        options:
          - full
          - incremental

jobs:
  export-database:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          lfs: true  # If using Git LFS for database file
          
      - name: Setup Python with UV
        uses: astral-sh/setup-uv@v4
        with:
          python-version: '3.12'
          
      - name: Install dependencies
        run: uv sync --all-extras
        
      - name: Verify database file exists
        run: |
          if [ ! -f "data/onepiece.duckdb" ]; then
            echo "Error: Database file not found"
            exit 1
          fi
          ls -lh data/onepiece.duckdb
          
      - name: Export to Supabase
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          SUPABASE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}
        run: |
          MODE="${{ github.event.inputs.mode || 'incremental' }}"
          uv run onepieceofdata export-supabase --mode $MODE --verbose
          
      - name: Verify export
        env:
          SUPABASE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}
        run: |
          uv run onepieceofdata sync-status
          
      - name: Notify on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '❌ Supabase export failed. Check workflow logs for details.'
            })
```

**Database Change Detection:**

Option 1: Simple (check if file changed in commit)
- GitHub Actions automatically detects file changes in `paths:`
- No additional script needed

Option 2: Advanced (check file hash)
```bash
# .github/scripts/check-db-changes.sh
#!/bin/bash
CURRENT_HASH=$(md5sum data/onepiece.duckdb | awk '{print $1}')
PREVIOUS_HASH=$(git show HEAD^:data/onepiece.duckdb | md5sum | awk '{print $1}')

if [ "$CURRENT_HASH" != "$PREVIOUS_HASH" ]; then
    echo "Database changed"
    echo "changed=true" >> $GITHUB_OUTPUT
else
    echo "Database unchanged"
    echo "changed=false" >> $GITHUB_OUTPUT
fi
```

### 5. CLI Commands Implementation

**Add to `src/onepieceofdata/cli.py`:**

```python
@main.command()
@click.option(
    '--mode',
    type=click.Choice(['full', 'incremental']),
    default='incremental',
    help='Export mode: full or incremental'
)
@click.option(
    '--tables',
    type=str,
    help='Comma-separated list of tables to export (default: all)'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Preview changes without actually exporting'
)
@click.option(
    '--verbose',
    is_flag=True,
    help='Show detailed progress'
)
def export_supabase(mode: str, tables: Optional[str], dry_run: bool, verbose: bool) -> None:
    """Export DuckDB database to Supabase PostgreSQL."""
    from .database.export import SupabaseExportManager
    
    # Validate configuration
    if not settings.supabase_db_url:
        click.echo("❌ Supabase connection not configured")
        click.echo("Set SUPABASE_DB_URL in .env file")
        sys.exit(1)
    
    # Parse table list
    table_list = tables.split(',') if tables else None
    
    # Initialize export manager
    with DatabaseManager() as db:
        exporter = SupabaseExportManager(db, settings.supabase_db_url)
        
        if dry_run:
            click.echo("🔍 DRY RUN MODE - No changes will be made")
        
        click.echo(f"📤 Exporting to Supabase ({mode} mode)...")
        
        # Run export
        if mode == 'full':
            result = exporter.export_full(tables=table_list, dry_run=dry_run)
        else:
            result = exporter.export_incremental(tables=table_list, dry_run=dry_run)
        
        # Display results
        click.echo(f"\n✅ Export completed!")
        click.echo(f"📊 Tables exported: {result.tables_exported}")
        click.echo(f"📝 Records exported: {result.total_records:,}")
        click.echo(f"⏱️  Duration: {result.duration_seconds:.2f}s")
        
        if result.errors:
            click.echo(f"⚠️  Errors: {len(result.errors)}")
            for error in result.errors:
                click.echo(f"  - {error}")


@main.command()
def sync_status() -> None:
    """Show Supabase sync status and statistics."""
    from .database.export import SupabaseExportManager
    
    if not settings.supabase_db_url:
        click.echo("❌ Supabase connection not configured")
        sys.exit(1)
    
    with DatabaseManager() as db:
        exporter = SupabaseExportManager(db, settings.supabase_db_url)
        status = exporter.get_sync_status()
        
        click.echo("📊 Supabase Sync Status")
        click.echo("=" * 50)
        
        for table_status in status.tables:
            click.echo(f"\n📋 {table_status.table_name}")
            click.echo(f"  Last sync: {table_status.last_sync or 'Never'}")
            click.echo(f"  DuckDB records: {table_status.duckdb_count:,}")
            click.echo(f"  PostgreSQL records: {table_status.postgres_count:,}")
            
            if table_status.duckdb_count == table_status.postgres_count:
                click.echo("  ✅ In sync")
            else:
                diff = table_status.duckdb_count - table_status.postgres_count
                click.echo(f"  ⚠️  Out of sync ({diff:+,} records)")
```

---

## Security Considerations

### 1. Credential Management

**DO NOT:**
- Commit credentials to Git
- Hardcode connection strings
- Use admin credentials for export

**DO:**
- Use environment variables
- Store secrets in GitHub Secrets
- Use service role keys (read/write only what's needed)
- Implement connection pooling limits

### 2. GitHub Secrets Setup

Required secrets in GitHub repository settings:
```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.xxxxx.supabase.co:5432/postgres
```

### 3. Network Security

**Supabase Configuration:**
- Enable Row Level Security (RLS) if needed
- Configure IP allowlist if available
- Use SSL/TLS connections (enforced by Supabase)

**Connection String:**
```python
# Always use SSL
SUPABASE_DB_URL = "postgresql://user:pass@host:5432/db?sslmode=require"
```

### 4. Error Handling

**Sensitive Information:**
```python
def sanitize_error(error: Exception) -> str:
    """Remove sensitive information from error messages."""
    error_str = str(error)
    # Remove connection strings
    error_str = re.sub(r'postgresql://[^@]+@', 'postgresql://***@', error_str)
    # Remove API keys
    error_str = re.sub(r'eyJ[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*', '***', error_str)
    return error_str
```

### 5. Rate Limiting

**Implement in export manager:**
```python
class SupabaseExportManager:
    def __init__(self, ...):
        self.batch_size = 1000  # Prevent overwhelming Supabase
        self.batch_delay = 0.1  # Small delay between batches
```

---

## Testing Strategy

### 1. Unit Tests

**File: `tests/test_export.py`**

```python
import pytest
from onepieceofdata.database.export import SupabaseExportManager
from onepieceofdata.database.schema_mapper import SchemaMapper

def test_schema_mapper_type_conversion():
    """Test DuckDB to PostgreSQL type mapping."""
    mapper = SchemaMapper()
    assert mapper.map_column_type('INTEGER') == 'INTEGER'
    assert mapper.map_column_type('VARCHAR') == 'TEXT'
    assert mapper.map_column_type('BIGINT') == 'BIGINT'

def test_create_postgres_schema():
    """Test PostgreSQL CREATE TABLE generation."""
    mapper = SchemaMapper()
    sql = mapper.create_postgres_schema('chapter')
    assert 'CREATE TABLE' in sql
    assert 'number INTEGER PRIMARY KEY' in sql

@pytest.mark.integration
def test_full_export(test_db, test_postgres):
    """Test full database export."""
    exporter = SupabaseExportManager(test_db, test_postgres)
    result = exporter.export_full(tables=['chapter'])
    assert result.success
    assert result.tables_exported == 1
```

### 2. Integration Tests

**Setup test Supabase instance:**
```python
@pytest.fixture
def test_supabase_connection():
    """Create temporary test database."""
    # Option 1: Use Supabase project
    # Option 2: Use local PostgreSQL with Docker
    engine = create_engine(TEST_POSTGRES_URL)
    yield engine
    # Cleanup
    with engine.connect() as conn:
        conn.execute("DROP SCHEMA IF EXISTS onepiece CASCADE")
```

### 3. Performance Tests

**Benchmark export speed:**
```python
def test_export_performance():
    """Ensure export completes within acceptable time."""
    start = time.time()
    exporter.export_full()
    duration = time.time() - start
    
    # Should complete in under 5 minutes for full database
    assert duration < 300
```

### 4. Schema Validation Tests

```python
def test_schema_consistency():
    """Verify PostgreSQL schema matches DuckDB."""
    exporter = SupabaseExportManager(db, pg_url)
    result = exporter.verify_schema()
    
    assert result.tables_match
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 0
```

### 5. Manual Testing Checklist

```
Manual Testing Checklist:
□ Full export creates all tables correctly
□ Incremental export detects changes
□ Foreign key constraints are preserved
□ Data integrity is maintained (no data loss)
□ Large datasets export without timeout
□ Error handling works for network issues
□ Dry run mode doesn't modify database
□ CLI commands work as expected
□ GitHub Actions workflow triggers correctly
□ Secrets are properly masked in logs
□ Sync status command shows accurate information
```

---

## Challenges and Solutions

### Challenge 1: Schema Differences Between DuckDB and PostgreSQL

**Problem:**
- DuckDB and PostgreSQL have different type systems
- Constraint syntax differs
- Index creation differs

**Solution:**
```python
class SchemaMapper:
    """Intelligent schema translation."""
    
    TYPE_MAP = {
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'VARCHAR': 'TEXT',  # PostgreSQL TEXT is more flexible
        'DATE': 'DATE',
    }
    
    def create_table_sql(self, table_name: str) -> str:
        """Generate PostgreSQL-compatible CREATE TABLE."""
        # Read DuckDB schema
        schema = self.db.query(f"DESCRIBE {table_name}")
        
        # Build CREATE TABLE statement
        columns = []
        for _, row in schema.iterrows():
            col_name = row['column_name']
            col_type = self.TYPE_MAP.get(row['column_type'], 'TEXT')
            nullable = '' if row['null'] == 'YES' else 'NOT NULL'
            primary = 'PRIMARY KEY' if row['key'] == 'PRI' else ''
            
            columns.append(f"{col_name} {col_type} {nullable} {primary}".strip())
        
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
```

### Challenge 2: Large Database Size

**Problem:**
- Exporting 6.3MB database with 1000+ chapters
- Memory limitations
- Network timeouts

**Solution:**
```python
def export_table_batched(self, table_name: str, batch_size: int = 1000):
    """Export table in batches to avoid memory issues."""
    total_rows = self.db.query(f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0]
    
    with self.engine.begin() as conn:
        for offset in range(0, total_rows, batch_size):
            # Read batch from DuckDB
            batch_df = self.db.query(
                f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
            )
            
            # Write to PostgreSQL
            batch_df.to_sql(
                table_name,
                conn,
                if_exists='append',
                index=False,
                method='multi'  # Faster multi-row inserts
            )
            
            logger.info(f"Exported {offset + len(batch_df)}/{total_rows} records")
```

### Challenge 3: Detecting Database Changes in Git

**Problem:**
- DuckDB is a binary file
- Git can't show meaningful diffs
- Need to trigger export only when database actually changes

**Solution 1: Use Git LFS and file hash**
```yaml
# In GitHub Actions
- name: Check if database changed
  id: db_check
  run: |
    git diff --name-only HEAD^ HEAD | grep -q 'onepiece.duckdb' && echo "changed=true" >> $GITHUB_OUTPUT || echo "changed=false" >> $GITHUB_OUTPUT

- name: Export if changed
  if: steps.db_check.outputs.changed == 'true'
  run: uv run onepieceofdata export-supabase
```

**Solution 2: Add metadata file**
```python
# Generate metadata when database updates
def save_db_metadata():
    """Save database metadata for change tracking."""
    with DatabaseManager() as db:
        stats = db.get_database_stats()
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'tables': stats,
            'hash': calculate_db_hash('data/onepiece.duckdb')
        }
        with open('data/onepiece_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
```

### Challenge 4: Handling Foreign Key Constraints

**Problem:**
- Tables must be created and populated in correct order
- Foreign key violations if order is wrong

**Solution:**
```python
# Define table dependency order
TABLE_ORDER = [
    'volume',      # No dependencies
    'character',   # No dependencies
    'saga',        # No dependencies
    'chapter',     # References volume
    'arc',         # References saga and chapter
    'coc',         # References chapter and character
    'cov',         # References volume and character
]

def export_full(self):
    """Export tables in dependency order."""
    for table in TABLE_ORDER:
        logger.info(f"Exporting table: {table}")
        self.export_table(table)
```

### Challenge 5: Incremental Update Detection

**Problem:**
- DuckDB doesn't have built-in change tracking
- No update timestamps on records
- Need efficient way to detect changes

**Solution: Multi-level strategy**
```python
def detect_changes(self, table_name: str) -> ChangeDetectionResult:
    """Detect changes using multiple strategies."""
    
    # Level 1: Quick row count check
    duckdb_count = self.get_duckdb_count(table_name)
    postgres_count = self.get_postgres_count(table_name)
    
    if duckdb_count != postgres_count:
        return ChangeDetectionResult(changed=True, strategy='count_mismatch')
    
    # Level 2: Check last sync time
    last_sync = self.metadata.get_last_sync(table_name)
    db_modified = os.path.getmtime(self.db.db_path)
    
    if last_sync and datetime.fromtimestamp(db_modified) < last_sync:
        return ChangeDetectionResult(changed=False, strategy='timestamp')
    
    # Level 3: Checksum comparison (expensive, only if needed)
    duckdb_checksum = self.calculate_table_checksum(table_name, source='duckdb')
    postgres_checksum = self.calculate_table_checksum(table_name, source='postgres')
    
    if duckdb_checksum != postgres_checksum:
        return ChangeDetectionResult(changed=True, strategy='checksum_mismatch')
    
    return ChangeDetectionResult(changed=False, strategy='checksum_match')
```

### Challenge 6: Error Recovery and Retry Logic

**Problem:**
- Network failures during export
- Partial exports leaving database in inconsistent state
- Long-running exports timing out

**Solution: Transactional export with retries**
```python
from tenacity import retry, stop_after_attempt, wait_exponential

class SupabaseExportManager:
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def export_table(self, table_name: str):
        """Export table with automatic retry on failure."""
        try:
            with self.engine.begin() as transaction:
                # Truncate existing data
                transaction.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                
                # Export in batches
                self.export_table_batched(table_name, transaction)
                
                # Update metadata
                self.metadata.update_sync_metadata(
                    table_name,
                    self.get_duckdb_count(table_name)
                )
                
                # Transaction commits automatically if no exception
                logger.success(f"Successfully exported {table_name}")
                
        except Exception as e:
            logger.error(f"Failed to export {table_name}: {e}")
            raise  # Let retry decorator handle it
```

---

## Recommended Library Choices

### Primary Recommendation: SQLAlchemy + psycopg2

**Rationale:**
1. **SQLAlchemy**: Industry standard ORM with excellent PostgreSQL support
2. **psycopg2-binary**: Mature, fast PostgreSQL adapter
3. **pandas integration**: Works seamlessly with your existing DuckDB code

**Pros:**
- Well-documented and widely used
- Excellent type mapping support
- Built-in connection pooling
- Transaction management
- Works with pandas DataFrames

**Cons:**
- Additional dependency
- Learning curve if unfamiliar

### Alternative: Supabase Python Client

**Use Case:** If you want to use Supabase's REST API instead of direct PostgreSQL connection

**Pros:**
- Simpler API
- Built-in authentication
- Automatic API key handling

**Cons:**
- Less control over schema
- Potential performance overhead
- Rate limiting on API tier

**Recommendation:** Use direct PostgreSQL connection (SQLAlchemy) for better performance and control.

---

## Configuration Management Best Practices

### 1. Environment-Specific Configs

```python
# src/onepieceofdata/config/settings.py

class Settings(BaseSettings):
    # ... existing config ...
    
    # Supabase Configuration
    supabase_url: Optional[str] = Field(default=None, alias="SUPABASE_URL")
    supabase_key: Optional[str] = Field(default=None, alias="SUPABASE_KEY")
    supabase_db_url: Optional[str] = Field(default=None, alias="SUPABASE_DB_URL")
    
    # Export Configuration
    export_mode: str = Field(default="incremental", alias="EXPORT_MODE")
    export_batch_size: int = Field(default=1000, alias="EXPORT_BATCH_SIZE")
    export_enabled: bool = Field(default=True, alias="EXPORT_ENABLED")
    
    @property
    def supabase_configured(self) -> bool:
        """Check if Supabase is configured."""
        return all([self.supabase_url, self.supabase_key, self.supabase_db_url])
```

### 2. Validation

```python
def validate_supabase_config(settings: Settings) -> None:
    """Validate Supabase configuration."""
    if not settings.supabase_db_url:
        raise ValueError("SUPABASE_DB_URL not configured")
    
    # Test connection
    try:
        engine = create_engine(settings.supabase_db_url)
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.success("Supabase connection validated")
    except Exception as e:
        raise ValueError(f"Failed to connect to Supabase: {e}")
```

---

## Makefile Integration

**Add to Makefile:**

```makefile
# Export to Supabase
export-supabase:
	@echo "📤 Exporting to Supabase (incremental mode)..."
	$(UV) run onepieceofdata export-supabase --mode incremental

export-supabase-full:
	@echo "📤 Exporting to Supabase (full mode)..."
	$(UV) run onepieceofdata export-supabase --mode full

export-supabase-dry-run:
	@echo "🔍 Dry run: Exporting to Supabase..."
	$(UV) run onepieceofdata export-supabase --dry-run

sync-status:
	@echo "📊 Checking Supabase sync status..."
	$(UV) run onepieceofdata sync-status

# Combined workflow: scrape + parse + export
run-full-pipeline-with-export:
	@echo "🚀 Running complete pipeline with Supabase export..."
	$(MAKE) run-full-pipeline
	$(MAKE) export-supabase
```

---

## Expected Performance Metrics

Based on current database size (6.3MB, ~1164 chapters):

**Full Export:**
- Expected duration: 30-60 seconds
- Network transfer: ~6-10 MB (including overhead)
- Database operations: ~2000-3000 INSERT statements

**Incremental Export (no changes):**
- Expected duration: <5 seconds
- Operations: Metadata check only

**Incremental Export (with changes):**
- Expected duration: 10-30 seconds
- Operations: Checksum comparison + selective updates

---

## Rollout Plan

### Week 1: Foundation
- [ ] Add dependencies to pyproject.toml
- [ ] Create export module structure
- [ ] Implement SchemaMapper
- [ ] Implement basic full export

### Week 2: Core Features
- [ ] Add MetadataTracker
- [ ] Implement incremental export
- [ ] Add CLI commands
- [ ] Write unit tests

### Week 3: Automation
- [ ] Create GitHub Actions workflow
- [ ] Setup secrets in repository
- [ ] Test end-to-end workflow
- [ ] Add error notifications

### Week 4: Polish
- [ ] Integration testing
- [ ] Documentation
- [ ] Performance optimization
- [ ] Production deployment

---

## Success Criteria

✅ **Must Have:**
1. Manual export works reliably
2. Incremental updates detect changes correctly
3. All 7 tables export with correct schema
4. Foreign key constraints preserved
5. GitHub Actions workflow triggers on DB changes
6. Credentials managed securely
7. Basic error handling and logging

✅ **Nice to Have:**
1. Export performance <60s for full database
2. Comprehensive test coverage (>80%)
3. Detailed documentation
4. Monitoring and alerting
5. Rollback capability
6. Data validation and integrity checks

---

## Monitoring and Observability

### Metrics to Track:
1. Export duration
2. Record counts per table
3. Error rates
4. Last successful export timestamp
5. Data drift (DuckDB vs PostgreSQL)

### Logging:
```python
logger.info(f"Starting export: mode={mode}, tables={tables}")
logger.info(f"Table {table}: {count} records exported in {duration}s")
logger.error(f"Export failed: {sanitize_error(error)}")
logger.success(f"Export completed: {total_records} records in {total_duration}s")
```

---

## Future Enhancements

1. **Real-time sync**: Use triggers or polling for immediate updates
2. **Bi-directional sync**: Allow Supabase → DuckDB sync
3. **Conflict resolution**: Handle concurrent updates
4. **Data transformation**: Apply transformations during export
5. **Multi-environment support**: Dev/staging/production Supabase instances
6. **Webhooks**: Notify other services after successful export
7. **Backup and restore**: Automated backups before export

---

## Resources and References

### Documentation:
- SQLAlchemy: https://docs.sqlalchemy.org/
- Supabase Python: https://supabase.com/docs/reference/python
- DuckDB: https://duckdb.org/docs/
- GitHub Actions: https://docs.github.com/actions

### Example Projects:
- [SQLAlchemy migrations](https://alembic.sqlalchemy.org/)
- [Supabase Python examples](https://github.com/supabase/supabase-py)

---

## Contact and Support

For questions or issues with this implementation:
1. Check logs in `logs/onepieceofdata.log`
2. Run `uv run onepieceofdata sync-status` for diagnostics
3. Review GitHub Actions workflow logs
4. Consult Supabase dashboard for database status

---

*Last updated: 2025-11-10*
*Version: 1.0*
