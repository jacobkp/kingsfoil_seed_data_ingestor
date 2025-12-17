"""
File validation service.
Validates headers, data types, and row counts.
"""

import pandas as pd
from typing import Optional
import asyncpg

from app.services.transformers import parse_numeric, parse_date


async def validate_file(
    conn: asyncpg.Connection,
    source_id: int,
    df: pd.DataFrame,
    header_row_index: int,
    column_map: dict,
) -> dict:
    """
    Validate file contents after header detection.

    Checks:
    1. Required columns present (already done by header detection)
    2. Data rows exist
    3. Sample data type validation (warnings only)
    4. Row count sanity check against previous upload

    Args:
        conn: Database connection
        source_id: Data source ID
        df: DataFrame
        header_row_index: Index of header row
        column_map: Mapping of internal_name -> file_header

    Returns:
        {
            'passed': bool,
            'errors': [str],
            'warnings': [str]
        }
    """
    errors = []
    warnings = []

    data_start_row = header_row_index + 1
    data_rows = len(df) - data_start_row

    # Check data rows exist
    if data_rows <= 0:
        errors.append("No data rows found after header row")
        return {"passed": False, "errors": errors, "warnings": warnings}

    # Get reference row count from previous uploads (if any)
    prev_upload = await conn.fetchrow(
        """
        SELECT record_count FROM meta.data_versions
        WHERE source_id = $1 AND status = 'completed'
        ORDER BY effective_date DESC
        LIMIT 1
        """,
        source_id,
    )

    if prev_upload and prev_upload["record_count"]:
        prev_count = prev_upload["record_count"]

        # Warn if row count differs significantly (>50% change)
        if data_rows < prev_count * 0.5:
            warnings.append(
                f"Row count ({data_rows:,}) is much lower than previous upload ({prev_count:,}). "
                "Please verify this is the correct file."
            )
        elif data_rows > prev_count * 1.5:
            warnings.append(
                f"Row count ({data_rows:,}) is much higher than previous upload ({prev_count:,}). "
                "This may be expected for a new version."
            )

    # Sample data type validation
    type_warnings = await _validate_data_types(
        conn, source_id, df, header_row_index, column_map
    )
    warnings.extend(type_warnings)

    passed = len(errors) == 0
    return {"passed": passed, "errors": errors, "warnings": warnings}


async def _validate_data_types(
    conn: asyncpg.Connection,
    source_id: int,
    df: pd.DataFrame,
    header_row_index: int,
    column_map: dict,
    sample_size: int = 100,
) -> list[str]:
    """
    Validate data types in sample rows.

    Args:
        conn: Database connection
        source_id: Data source ID
        df: DataFrame
        header_row_index: Header row index
        column_map: Column mapping
        sample_size: Number of rows to sample

    Returns:
        List of warning messages
    """
    warnings = []

    # Get canonical columns with their expected types
    columns = await conn.fetch(
        """
        SELECT internal_name, data_type
        FROM meta.canonical_columns
        WHERE source_id = $1
        """,
        source_id,
    )

    type_map = {row["internal_name"]: row["data_type"] for row in columns}

    # Build header index map
    header_values = [str(val).strip() for val in df.iloc[header_row_index].tolist()]
    header_to_idx = {val: idx for idx, val in enumerate(header_values)}

    data_start_row = header_row_index + 1
    sample_end = min(data_start_row + sample_size, len(df))

    # Track which columns have already generated a warning
    warned_columns = set()

    for internal_name, file_header in column_map.items():
        if internal_name in warned_columns:
            continue

        expected_type = type_map.get(internal_name)
        if not expected_type:
            continue

        col_idx = header_to_idx.get(file_header)
        if col_idx is None:
            continue

        # Check sample values
        if expected_type in ["NUMERIC", "INTEGER"]:
            for row_idx in range(data_start_row, sample_end):
                val = str(df.iloc[row_idx, col_idx]).strip()
                if val and val not in ["*", "", "NULL", "N/A"]:
                    parsed = parse_numeric(val)
                    if parsed is None:
                        warnings.append(
                            f"Column '{internal_name}' contains non-numeric value '{val}' "
                            f"at row {row_idx + 1}"
                        )
                        warned_columns.add(internal_name)
                        break

        elif expected_type == "DATE":
            for row_idx in range(data_start_row, sample_end):
                val = str(df.iloc[row_idx, col_idx]).strip()
                if val and val not in ["*", "", "NULL", "N/A"]:
                    parsed = parse_date(val)
                    if parsed is None:
                        warnings.append(
                            f"Column '{internal_name}' contains unparseable date '{val}' "
                            f"at row {row_idx + 1}"
                        )
                        warned_columns.add(internal_name)
                        break

    return warnings


async def check_duplicate_file(
    conn: asyncpg.Connection,
    source_id: int,
    file_hash: str,
) -> Optional[dict]:
    """
    Check if a file with the same hash has already been uploaded.

    Args:
        conn: Database connection
        source_id: Data source ID
        file_hash: SHA-256 hash of file contents

    Returns:
        Dict with previous upload info if duplicate, None otherwise
    """
    # Only check completed uploads - allow re-uploading files that failed ingestion
    existing = await conn.fetchrow(
        """
        SELECT id, version_label, variant, imported_at, file_name
        FROM meta.data_versions
        WHERE source_id = $1 AND file_hash = $2 AND status = 'completed'
        """,
        source_id,
        file_hash,
    )

    if existing:
        return {
            "version_id": existing["id"],
            "version_label": existing["version_label"],
            "variant": existing["variant"],
            "imported_at": existing["imported_at"],
            "file_name": existing["file_name"],
        }

    return None


def validate_file_extension(filename: str, allowed_extensions: list[str]) -> Optional[str]:
    """
    Validate that the file has an allowed extension.

    Args:
        filename: Original filename
        allowed_extensions: List of allowed extensions (without dots)

    Returns:
        Error message if invalid, None if valid
    """
    if not filename:
        return "No filename provided"

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if not ext:
        return "File has no extension"

    if ext not in allowed_extensions:
        return f"File type '.{ext}' not supported. Allowed: {', '.join(allowed_extensions)}"

    return None
