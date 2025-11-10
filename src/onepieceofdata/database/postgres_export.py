"""PostgreSQL export functionality for DuckDB database.

This module handles exporting data from DuckDB to PostgreSQL (including Supabase).
Supports both full and incremental exports with sync tracking.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time

import duckdb
from loguru import logger
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

from ..config import settings
from .schema_mapper import SchemaMapper


class PostgresExporter:
    """Handles export from DuckDB to PostgreSQL."""

    def __init__(
        self,
        duckdb_path: Optional[str] = None,
        postgres_url: Optional[str] = None,
        batch_size: Optional[int] = None
    ):
        """Initialize the exporter.

        Args:
            duckdb_path: Path to DuckDB database (defaults to settings)
            postgres_url: PostgreSQL connection URL (defaults to settings)
            batch_size: Number of records per batch (defaults to settings)
        """
        self.duckdb_path = duckdb_path or str(settings.database_path)
        self.postgres_url = postgres_url or settings.postgres_connection_url
        self.batch_size = batch_size or settings.export_batch_size
        self.sync_metadata_path = settings.sync_metadata_path

        self.duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.pg_engine: Optional[Engine] = None

    def connect(self) -> None:
        """Connect to both DuckDB and PostgreSQL."""
        logger.info("Connecting to DuckDB...")
        self.duckdb_conn = duckdb.connect(self.duckdb_path, read_only=True)
        logger.success("DuckDB connection established")

        logger.info("Connecting to PostgreSQL...")
        self.pg_engine = create_engine(
            self.postgres_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        # Test connection
        with self.pg_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("PostgreSQL connection established")

    def disconnect(self) -> None:
        """Close database connections."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            logger.info("DuckDB connection closed")

        if self.pg_engine:
            self.pg_engine.dispose()
            logger.info("PostgreSQL connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def get_table_schema(self, table_name: str) -> List[Tuple[str, str, bool]]:
        """Get schema for a table from DuckDB.

        Args:
            table_name: Name of the table

        Returns:
            List of (column_name, type, is_nullable) tuples
        """
        if not self.duckdb_conn:
            raise RuntimeError("Not connected to DuckDB")

        result = self.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()

        # DuckDB DESCRIBE returns: (column_name, column_type, null, key, default, extra)
        columns = []
        for row in result:
            col_name = row[0]
            col_type = row[1]
            is_nullable = row[2].upper() == 'YES'
            columns.append((col_name, col_type, is_nullable))

        return columns

    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of primary key column names
        """
        if not self.duckdb_conn:
            raise RuntimeError("Not connected to DuckDB")

        result = self.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()

        primary_keys = []
        for row in result:
            col_name = row[0]
            key_type = row[3]  # 'PRI' for primary keys
            if key_type == 'PRI':
                primary_keys.append(col_name)

        return primary_keys

    def get_row_count(self, table_name: str, from_postgres: bool = False) -> int:
        """Get row count for a table.

        Args:
            table_name: Name of the table
            from_postgres: If True, query PostgreSQL; else query DuckDB

        Returns:
            Number of rows
        """
        if from_postgres:
            if not self.pg_engine:
                raise RuntimeError("Not connected to PostgreSQL")
            with self.pg_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar() or 0
        else:
            if not self.duckdb_conn:
                raise RuntimeError("Not connected to DuckDB")
            result = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0] if result else 0

    def create_table(self, table_name: str, drop_if_exists: bool = False) -> None:
        """Create a table in PostgreSQL based on DuckDB schema.

        Args:
            table_name: Name of the table to create
            drop_if_exists: Whether to drop the table if it exists
        """
        if not self.pg_engine:
            raise RuntimeError("Not connected to PostgreSQL")

        logger.info(f"Creating table {table_name}...")

        # Get schema from DuckDB
        columns = self.get_table_schema(table_name)
        primary_keys = self.get_primary_keys(table_name)

        # Build CREATE TABLE SQL
        create_sql = SchemaMapper.build_create_table_sql(
            table_name, columns, primary_keys, drop_if_exists
        )

        # Execute
        with self.pg_engine.begin() as conn:
            for statement in create_sql.split(';'):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))

        logger.success(f"Table {table_name} created successfully")

    def export_table(
        self,
        table_name: str,
        truncate: bool = False,
        show_progress: bool = True
    ) -> Dict[str, any]:
        """Export a single table from DuckDB to PostgreSQL.

        Args:
            table_name: Name of the table to export
            truncate: Whether to truncate the table before inserting
            show_progress: Whether to show progress logs

        Returns:
            Dictionary with export statistics
        """
        if not self.duckdb_conn or not self.pg_engine:
            raise RuntimeError("Not connected to databases")

        start_time = time.time()
        logger.info(f"Exporting table: {table_name}")

        # Get total rows
        total_rows = self.get_row_count(table_name, from_postgres=False)
        if total_rows == 0:
            logger.warning(f"Table {table_name} is empty, skipping")
            return {'table': table_name, 'rows_exported': 0, 'duration': 0, 'status': 'skipped'}

        # Truncate if requested
        if truncate:
            with self.pg_engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
            logger.info(f"Truncated table {table_name}")

        # Get column names
        columns = self.get_table_schema(table_name)
        col_names = [col[0] for col in columns]
        col_list = ', '.join(col_names)

        # Export in batches
        rows_exported = 0
        offset = 0

        while offset < total_rows:
            # Fetch batch from DuckDB
            query = f"SELECT {col_list} FROM {table_name} LIMIT {self.batch_size} OFFSET {offset}"
            batch = self.duckdb_conn.execute(query).fetchall()

            if not batch:
                break

            # Prepare insert statement
            placeholders = ', '.join([f':{col}' for col in col_names])
            insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

            # Insert batch into PostgreSQL
            with self.pg_engine.begin() as conn:
                for row in batch:
                    row_dict = dict(zip(col_names, row))
                    conn.execute(text(insert_sql), row_dict)

            rows_exported += len(batch)
            offset += self.batch_size

            if show_progress and offset % (self.batch_size * 10) == 0:
                progress = (offset / total_rows) * 100
                logger.info(f"  Progress: {offset}/{total_rows} ({progress:.1f}%)")

        duration = time.time() - start_time
        logger.success(f"Exported {rows_exported} rows from {table_name} in {duration:.2f}s")

        return {
            'table': table_name,
            'rows_exported': rows_exported,
            'duration': duration,
            'status': 'success'
        }

    def export_all(
        self,
        mode: str = 'full',
        tables: Optional[List[str]] = None,
        show_progress: bool = True
    ) -> Dict[str, any]:
        """Export all tables from DuckDB to PostgreSQL.

        Args:
            mode: Export mode ('full' or 'incremental')
            tables: Specific tables to export (None = all tables)
            show_progress: Whether to show progress logs

        Returns:
            Dictionary with export statistics
        """
        if not self.duckdb_conn or not self.pg_engine:
            raise RuntimeError("Not connected to databases")

        start_time = time.time()
        logger.info(f"Starting {mode} export to PostgreSQL")

        # Get tables to export
        if tables:
            export_tables = [t for t in SchemaMapper.get_table_order() if t in tables]
        else:
            export_tables = SchemaMapper.get_table_order()

        # Determine which tables need export
        if mode == 'incremental':
            export_tables = self._get_tables_to_update(export_tables)
            if not export_tables:
                logger.info("No tables need updating")
                return {
                    'status': 'success',
                    'mode': mode,
                    'tables_exported': 0,
                    'total_rows': 0,
                    'duration': 0,
                    'message': 'No changes detected'
                }

        # Create tables if they don't exist
        for table_name in export_tables:
            if not self._table_exists(table_name):
                self.create_table(table_name, drop_if_exists=False)

        # Export each table
        results = []
        total_rows = 0

        for table_name in export_tables:
            try:
                result = self.export_table(
                    table_name,
                    truncate=(mode == 'full'),
                    show_progress=show_progress
                )
                results.append(result)
                total_rows += result['rows_exported']
            except Exception as e:
                logger.error(f"Failed to export table {table_name}: {str(e)}")
                results.append({
                    'table': table_name,
                    'rows_exported': 0,
                    'duration': 0,
                    'status': 'failed',
                    'error': str(e)
                })

        # Add foreign keys (only in full mode)
        if mode == 'full':
            self._add_foreign_keys(export_tables)

        # Update sync metadata
        if settings.export_enable_sync_tracking:
            self._save_sync_metadata(export_tables)

        duration = time.time() - start_time
        logger.success(f"Export completed in {duration:.2f}s: {total_rows} total rows")

        return {
            'status': 'success',
            'mode': mode,
            'tables_exported': len([r for r in results if r['status'] == 'success']),
            'total_rows': total_rows,
            'duration': duration,
            'tables': results
        }

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in PostgreSQL.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists
        """
        if not self.pg_engine:
            return False

        inspector = inspect(self.pg_engine)
        return table_name in inspector.get_table_names()

    def _get_tables_to_update(self, tables: List[str]) -> List[str]:
        """Get list of tables that need updating (incremental mode).

        Args:
            tables: List of table names to check

        Returns:
            List of tables that need updating
        """
        tables_to_update = []

        # Load last sync metadata
        metadata = self._load_sync_metadata()

        for table_name in tables:
            # Check if table exists in PostgreSQL
            if not self._table_exists(table_name):
                logger.info(f"Table {table_name} doesn't exist in PostgreSQL, will create")
                tables_to_update.append(table_name)
                continue

            # Compare row counts
            duckdb_count = self.get_row_count(table_name, from_postgres=False)
            postgres_count = self.get_row_count(table_name, from_postgres=True)

            if duckdb_count != postgres_count:
                logger.info(f"Table {table_name} has changed: DuckDB={duckdb_count}, PostgreSQL={postgres_count}")
                tables_to_update.append(table_name)
            else:
                logger.debug(f"Table {table_name} unchanged ({duckdb_count} rows)")

        return tables_to_update

    def _add_foreign_keys(self, tables: List[str]) -> None:
        """Add foreign key constraints to tables.

        Args:
            tables: List of table names
        """
        logger.info("Adding foreign key constraints...")

        for table_name in tables:
            fk_statements = SchemaMapper.build_foreign_key_sql(table_name)
            if not fk_statements:
                continue

            with self.pg_engine.begin() as conn:
                for statement in fk_statements:
                    try:
                        conn.execute(text(statement))
                        logger.debug(f"Added FK for {table_name}")
                    except Exception as e:
                        # FK might already exist, that's okay
                        logger.debug(f"FK constraint skipped for {table_name}: {str(e)}")

    def _load_sync_metadata(self) -> Dict:
        """Load sync metadata from file.

        Returns:
            Metadata dictionary
        """
        if not self.sync_metadata_path.exists():
            return {}

        try:
            with open(self.sync_metadata_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load sync metadata: {str(e)}")
            return {}

    def _save_sync_metadata(self, tables: List[str]) -> None:
        """Save sync metadata to file.

        Args:
            tables: List of tables that were synced
        """
        metadata = self._load_sync_metadata()

        # Update metadata
        metadata['last_sync'] = datetime.now().isoformat()
        metadata['tables'] = {}

        for table_name in tables:
            try:
                row_count = self.get_row_count(table_name, from_postgres=True)
                metadata['tables'][table_name] = {
                    'row_count': row_count,
                    'last_synced': datetime.now().isoformat()
                }
            except Exception as e:
                logger.warning(f"Failed to get row count for {table_name}: {str(e)}")

        # Save to file
        try:
            with open(self.sync_metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Sync metadata saved to {self.sync_metadata_path}")
        except Exception as e:
            logger.error(f"Failed to save sync metadata: {str(e)}")

    def get_sync_status(self) -> Dict:
        """Get current sync status.

        Returns:
            Dictionary with sync status information
        """
        if not self.duckdb_conn or not self.pg_engine:
            raise RuntimeError("Not connected to databases")

        metadata = self._load_sync_metadata()

        status = {
            'last_sync': metadata.get('last_sync'),
            'tables': []
        }

        for table_name in SchemaMapper.get_table_order():
            try:
                duckdb_count = self.get_row_count(table_name, from_postgres=False)
                postgres_count = self.get_row_count(table_name, from_postgres=True) if self._table_exists(table_name) else 0

                table_metadata = metadata.get('tables', {}).get(table_name, {})

                status['tables'].append({
                    'name': table_name,
                    'duckdb_rows': duckdb_count,
                    'postgres_rows': postgres_count,
                    'in_sync': duckdb_count == postgres_count,
                    'last_synced': table_metadata.get('last_synced')
                })
            except Exception as e:
                logger.error(f"Failed to get status for {table_name}: {str(e)}")
                status['tables'].append({
                    'name': table_name,
                    'error': str(e)
                })

        return status
