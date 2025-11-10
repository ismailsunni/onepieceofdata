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
    }

    # Tables in dependency order (respects foreign keys)
    # Tables should be exported in this order to avoid FK violations
    TABLE_ORDER = [
        'saga',      # No dependencies
        'arc',       # Depends on: saga
        'volume',    # No dependencies
        'chapter',   # Depends on: volume (nullable)
        'character', # No dependencies
        'coc',       # Depends on: chapter, character
        'cov',       # Depends on: volume, character
    ]

    @classmethod
    def map_type(cls, duckdb_type: str) -> str:
        """Map a DuckDB type to PostgreSQL type.

        Args:
            duckdb_type: The DuckDB column type

        Returns:
            The corresponding PostgreSQL type
        """
        # Handle types with parameters (e.g., VARCHAR(255))
        base_type = duckdb_type.split('(')[0].upper()

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
            'coc': [
                "ALTER TABLE coc ADD CONSTRAINT fk_coc_chapter FOREIGN KEY (chapter) REFERENCES chapter(number) ON DELETE CASCADE;",
                "ALTER TABLE coc ADD CONSTRAINT fk_coc_character FOREIGN KEY (character) REFERENCES character(id) ON DELETE CASCADE;"
            ],
            'cov': [
                "ALTER TABLE cov ADD CONSTRAINT fk_cov_volume FOREIGN KEY (volume) REFERENCES volume(number) ON DELETE CASCADE;",
                "ALTER TABLE cov ADD CONSTRAINT fk_cov_character FOREIGN KEY (character) REFERENCES character(id) ON DELETE CASCADE;"
            ]
        }

        return fk_definitions.get(table_name, [])
