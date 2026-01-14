"""Schema mapping utilities for DuckDB to PostgreSQL conversion."""

from typing import Dict, List, Tuple
from loguru import logger


class SchemaMapper:
    """Maps DuckDB schema to PostgreSQL schema."""

    # DuckDB to PostgreSQL type mapping
    TYPE_MAPPING = {
        'VARCHAR': 'TEXT',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'DOUBLE': 'DOUBLE PRECISION',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'BOOLEAN': 'BOOLEAN',
        'INTEGER[]': 'INTEGER[]',
        'TEXT[]': 'TEXT[]',
    }

    # Tables in dependency order (respects foreign keys)
    # Tables should be exported in this order to avoid FK violations
    # Note: 'coc' and 'cov' tables are excluded - appearances are denormalized into character table
    #       - 'coc' (chapter appearances) → chapter_list, volume_list, arc_list, saga_list
    #       - 'cov' (cover appearances) → cover_volume_list, cover_appearance_count
    TABLE_ORDER = [
        'saga',      # No dependencies
        'arc',       # Depends on: saga
        'volume',    # No dependencies
        'chapter',   # Depends on: volume (nullable)
        'character', # No dependencies (includes denormalized appearance analytics)
    ]

    @classmethod
    def map_type(cls, duckdb_type: str) -> str:
        """Map a DuckDB type to PostgreSQL type.

        Args:
            duckdb_type: The DuckDB column type

        Returns:
            The corresponding PostgreSQL type
        """
        # Normalize type to uppercase for comparison
        normalized_type = duckdb_type.upper()

        # Check for exact match first (handles array types like INTEGER[])
        if normalized_type in cls.TYPE_MAPPING:
            mapped_type = cls.TYPE_MAPPING[normalized_type]
            if normalized_type != mapped_type:
                logger.debug(f"Mapped DuckDB type {duckdb_type} -> PostgreSQL type {mapped_type}")
            return mapped_type

        # Handle types with parameters (e.g., VARCHAR(255))
        base_type = normalized_type.split('(')[0]
        mapped_type = cls.TYPE_MAPPING.get(base_type, duckdb_type)

        if base_type != mapped_type.split('(')[0]:
            logger.debug(f"Mapped DuckDB type {duckdb_type} -> PostgreSQL type {mapped_type}")

        return mapped_type

    @classmethod
    def get_table_order(cls) -> List[str]:
        """Get tables in dependency order for safe export.

        Returns:
            List of table names in order
        """
        return cls.TABLE_ORDER.copy()

    @classmethod
    def build_create_table_sql(
        cls,
        table_name: str,
        columns: List[Tuple[str, str, bool]],
        primary_keys: List[str],
        drop_if_exists: bool = False
    ) -> str:
        """Build CREATE TABLE SQL for PostgreSQL.

        Args:
            table_name: Name of the table
            columns: List of (column_name, duckdb_type, is_nullable) tuples
            primary_keys: List of primary key column names
            drop_if_exists: Whether to drop the table if it exists

        Returns:
            SQL statement to create the table
        """
        sql_parts = []

        # Drop table if requested
        if drop_if_exists:
            sql_parts.append(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Create table
        col_definitions = []
        for col_name, col_type, is_nullable in columns:
            pg_type = cls.map_type(col_type)
            null_constraint = "" if is_nullable else " NOT NULL"
            col_definitions.append(f"    {col_name} {pg_type}{null_constraint}")

        # Add primary key constraint
        if primary_keys:
            pk_constraint = f"    PRIMARY KEY ({', '.join(primary_keys)})"
            col_definitions.append(pk_constraint)

        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        create_sql += ",\n".join(col_definitions)
        create_sql += "\n);"

        sql_parts.append(create_sql)

        return "\n".join(sql_parts)

    @classmethod
    def build_foreign_key_sql(cls, table_name: str) -> List[str]:
        """Build foreign key constraints for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of ALTER TABLE statements to add foreign keys
        """
        fk_definitions = {
            'arc': [
                "ALTER TABLE arc ADD CONSTRAINT fk_arc_saga FOREIGN KEY (saga_id) REFERENCES saga(saga_id) ON DELETE SET NULL;"
            ],
            # Note: 'coc' table is excluded from export - character appearances are denormalized
            'cov': [
                "ALTER TABLE cov ADD CONSTRAINT fk_cov_volume FOREIGN KEY (volume) REFERENCES volume(number) ON DELETE CASCADE;",
                "ALTER TABLE cov ADD CONSTRAINT fk_cov_character FOREIGN KEY (character) REFERENCES character(id) ON DELETE CASCADE;"
            ]
        }

        return fk_definitions.get(table_name, [])
