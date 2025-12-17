"""
Migration: Add part_count column and data_version_parts table.

Run this if you get the error:
    asyncpg.exceptions.UndefinedColumnError: column v.part_count does not exist

Usage:
    python -m scripts.migrate_add_part_count
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings


MIGRATION_SQL = """
-- Add part_count column to data_versions if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'meta'
          AND table_name = 'data_versions'
          AND column_name = 'part_count'
    ) THEN
        ALTER TABLE meta.data_versions ADD COLUMN part_count INT DEFAULT 1;
        RAISE NOTICE 'Added part_count column to meta.data_versions';
    ELSE
        RAISE NOTICE 'part_count column already exists';
    END IF;
END $$;

-- Create data_version_parts table if it doesn't exist
CREATE TABLE IF NOT EXISTS meta.data_version_parts (
    id SERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    file_size_bytes BIGINT,
    record_count INT,
    imported_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_version_part UNIQUE (data_version_id, part_number),
    CONSTRAINT uq_version_file UNIQUE (data_version_id, file_hash)
);
"""


async def run_migration():
    """Run the migration to add multi-part file support."""
    settings = get_settings()

    print("Connecting to database...")
    conn = await asyncpg.connect(settings.database_url)

    try:
        print("\nRunning migration: Add multi-part file support")
        print("=" * 60)

        await conn.execute(MIGRATION_SQL)

        # Verify the column exists
        has_column = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'meta'
                  AND table_name = 'data_versions'
                  AND column_name = 'part_count'
            )
        """)

        has_table = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'meta'
                  AND table_name = 'data_version_parts'
            )
        """)

        print(f"  part_count column exists: {has_column}")
        print(f"  data_version_parts table exists: {has_table}")

        if has_column and has_table:
            print("\nMigration completed successfully!")
        else:
            print("\nMigration may have failed - check database manually.")

    finally:
        await conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    asyncio.run(run_migration())
