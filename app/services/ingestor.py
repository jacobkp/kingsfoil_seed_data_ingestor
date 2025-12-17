"""
Data ingestion service.
Handles batch inserts, version management, and duplicate detection.
"""

import asyncpg
import pandas as pd
from datetime import datetime
from typing import Any, Optional

from app.services.transformers import (
    transform_value,
    clean_code,
    parse_mai_id,
    parse_mue_value,
    parse_deletion_date,
    parse_modifier_indicator,
    parse_prior_1996_flag,
)


# ============================================================
# Table Configuration
# ============================================================

# Maps source_code to table info
TABLE_CONFIG = {
    "PFS_RVU": {
        "table": "cms.pfs_rvu",
        "unique_keys": ["hcpcs_code", "modifier"],
        "columns": [
            "hcpcs_code", "modifier", "description", "status_code",
            "work_rvu", "non_fac_pe_rvu", "facility_pe_rvu", "mp_rvu",
            "non_fac_total", "facility_total", "pctc_indicator",
            "global_days", "conversion_factor",
        ],
    },
    "PFS_GPCI": {
        "table": "cms.pfs_gpci",
        "unique_keys": ["mac_locality"],
        "columns": ["mac_locality", "locality_name", "work_gpci", "pe_gpci", "mp_gpci"],
    },
    "PFS_LOCALITY": {
        "table": "cms.pfs_locality",
        "unique_keys": ["state_code", "county_code", "carrier_number", "locality_code"],
        "columns": [
            "state_code", "county_code", "county_name",
            "carrier_number", "locality_code", "mac_locality",
        ],
    },
    "PFS_ANES_CF": {
        "table": "cms.pfs_anes_cf",
        "unique_keys": ["mac_locality"],
        "columns": ["mac_locality", "locality_name", "anes_conversion_factor"],
    },
    "PFS_OPPS_CAP": {
        "table": "cms.pfs_opps_cap",
        "unique_keys": ["hcpcs_code"],
        "columns": ["hcpcs_code", "opps_cap_amount"],
    },
    "HCPCS": {
        "table": "cms.hcpcs_codes",
        "unique_keys": ["hcpcs_code"],
        "columns": [
            "hcpcs_code", "short_description", "long_description",
            "add_date", "effective_date", "termination_date",
            "betos_code", "coverage_code",
        ],
    },
    "NCCI_PTP": {
        "table": "cms.ncci_ptp",
        "unique_keys": ["comprehensive_code", "component_code"],
        "columns": [
            "comprehensive_code", "component_code", "modifier_indicator",
            "effective_date", "deletion_date", "rationale", "prior_1996_flag",
        ],
    },
    "NCCI_MUE_DME": {
        "table": "cms.ncci_mue",
        "unique_keys": ["hcpcs_code"],
        "columns": ["hcpcs_code", "mue_value", "mue_rationale", "mai_id", "mai_description"],
    },
    "NCCI_MUE_PRAC": {
        "table": "cms.ncci_mue",
        "unique_keys": ["hcpcs_code"],
        "columns": ["hcpcs_code", "mue_value", "mue_rationale", "mai_id", "mai_description"],
    },
    "NCCI_MUE_OPH": {
        "table": "cms.ncci_mue",
        "unique_keys": ["hcpcs_code"],
        "columns": ["hcpcs_code", "mue_value", "mue_rationale", "mai_id", "mai_description"],
    },
}


# ============================================================
# Version Management
# ============================================================


async def create_data_version(
    conn: asyncpg.Connection,
    source_id: int,
    version_label: str,
    variant: Optional[str],
    effective_date: datetime,
    file_name: str,
    file_hash: str,
    file_size_bytes: int,
    header_row_index: int,
) -> int:
    """
    Create a new data version record with status='processing'.
    """
    version_id = await conn.fetchval(
        """
        INSERT INTO meta.data_versions (
            source_id, version_label, variant, effective_date,
            file_name, file_hash, file_size_bytes, header_row_index,
            status, part_count, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'processing', 1, NOW())
        RETURNING id
        """,
        source_id, version_label, variant, effective_date,
        file_name, file_hash, file_size_bytes, header_row_index,
    )
    return version_id


async def get_existing_version(
    conn: asyncpg.Connection,
    source_id: int,
    version_label: str,
    variant: Optional[str],
) -> Optional[dict]:
    """
    Get existing completed version for appending multi-part files.
    """
    row = await conn.fetchrow(
        """
        SELECT id, record_count, part_count
        FROM meta.data_versions
        WHERE source_id = $1 AND version_label = $2 AND variant IS NOT DISTINCT FROM $3
          AND status = 'completed'
        """,
        source_id, version_label, variant,
    )
    if row:
        return dict(row)
    return None


async def add_version_part(
    conn: asyncpg.Connection,
    version_id: int,
    part_number: int,
    file_name: str,
    file_hash: str,
    file_size_bytes: int,
    record_count: int,
) -> int:
    """
    Add a part record for a multi-part upload.
    """
    part_id = await conn.fetchval(
        """
        INSERT INTO meta.data_version_parts (
            data_version_id, part_number, file_name, file_hash,
            file_size_bytes, record_count, imported_at
        ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING id
        """,
        version_id, part_number, file_name, file_hash, file_size_bytes, record_count,
    )
    return part_id


async def update_version_for_part(
    conn: asyncpg.Connection,
    version_id: int,
    additional_records: int,
) -> None:
    """
    Update version totals after adding a part.
    """
    await conn.execute(
        """
        UPDATE meta.data_versions
        SET record_count = COALESCE(record_count, 0) + $2,
            part_count = COALESCE(part_count, 1) + 1,
            imported_at = NOW()
        WHERE id = $1
        """,
        version_id, additional_records,
    )


async def update_version_status(
    conn: asyncpg.Connection,
    version_id: int,
    status: str,
    record_count: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update data version status."""
    if status == "completed":
        await conn.execute(
            """
            UPDATE meta.data_versions
            SET status = $2, record_count = $3, imported_at = NOW()
            WHERE id = $1
            """,
            version_id, status, record_count,
        )
    else:
        await conn.execute(
            """
            UPDATE meta.data_versions
            SET status = $2, error_message = $3
            WHERE id = $1
            """,
            version_id, status, error_message,
        )


async def mark_version_as_current(
    conn: asyncpg.Connection,
    source_id: int,
    version_id: int,
    variant: Optional[str] = None,
) -> None:
    """Mark a version as current, unmarking any previous current version."""
    if variant:
        await conn.execute(
            """
            UPDATE meta.data_versions
            SET is_current = FALSE
            WHERE source_id = $1 AND variant = $2 AND is_current = TRUE
            """,
            source_id, variant,
        )
    else:
        await conn.execute(
            """
            UPDATE meta.data_versions
            SET is_current = FALSE
            WHERE source_id = $1 AND is_current = TRUE
            """,
            source_id,
        )

    await conn.execute(
        """
        UPDATE meta.data_versions
        SET is_current = TRUE
        WHERE id = $1
        """,
        version_id,
    )


# ============================================================
# Ingestion Logging
# ============================================================


async def log_ingestion_event(
    conn: asyncpg.Connection,
    version_id: int,
    level: str,
    message: str,
    details: Optional[dict] = None,
) -> None:
    """Log an ingestion event to database."""
    import json
    await conn.execute(
        """
        INSERT INTO meta.ingestion_logs (data_version_id, log_level, message, details)
        VALUES ($1, $2, $3, $4)
        """,
        version_id, level, message,
        json.dumps(details) if details else None,
    )


# ============================================================
# Record Transformation
# ============================================================


def is_empty_row(row: pd.Series, threshold: float = 0.8) -> bool:
    """
    Check if a row is essentially empty (metadata/blank row after header).

    Args:
        row: DataFrame row
        threshold: Percentage of empty cells to consider row empty

    Returns:
        True if row is mostly empty
    """
    empty_count = sum(1 for val in row if str(val).strip() in ["", "nan", "NaN", "None"])
    return empty_count / len(row) >= threshold


def transform_record(
    row: pd.Series,
    column_map: dict[str, str],
    type_map: dict[str, str],
    header_to_idx: dict[str, int],
    source_code: str,
) -> dict[str, Any]:
    """Transform a DataFrame row into a record dict."""
    record = {}

    for internal_name, file_header in column_map.items():
        col_idx = header_to_idx.get(file_header)
        if col_idx is None:
            continue

        raw_value = str(row.iloc[col_idx]).strip() if col_idx < len(row) else ""
        data_type = type_map.get(internal_name, "TEXT")

        # Special handling for specific columns/sources
        if source_code.startswith("NCCI_MUE"):
            if internal_name == "mai_id":
                mai_desc_header = column_map.get("mai_description")
                if mai_desc_header:
                    mai_idx = header_to_idx.get(mai_desc_header)
                    if mai_idx is not None:
                        mai_raw = str(row.iloc[mai_idx]).strip()
                        record["mai_id"] = parse_mai_id(mai_raw)
                continue
            elif internal_name == "mue_value":
                record["mue_value"] = parse_mue_value(raw_value)
                continue

        elif source_code == "NCCI_PTP":
            if internal_name == "deletion_date":
                record["deletion_date"] = parse_deletion_date(raw_value)
                continue
            elif internal_name == "modifier_indicator":
                record["modifier_indicator"] = parse_modifier_indicator(raw_value)
                continue
            elif internal_name == "prior_1996_flag":
                record["prior_1996_flag"] = parse_prior_1996_flag(raw_value)
                continue

        # Handle code columns specially to preserve formatting
        if internal_name.endswith("_code"):
            record[internal_name] = clean_code(raw_value)
        else:
            record[internal_name] = transform_value(raw_value, data_type)

    return record


def validate_record(record: dict, unique_keys: list[str], row_number: int) -> tuple[bool, Optional[str]]:
    """
    Validate a record before insertion.

    Args:
        record: Record dict
        unique_keys: List of required unique key columns
        row_number: Original row number in file (for error message)

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check that all unique key columns have values
    for key in unique_keys:
        if record.get(key) is None:
            return False, f"Row {row_number}: Missing required key column '{key}'"

    return True, None


# ============================================================
# Batch Insert with Row-Level Error Handling
# ============================================================


async def batch_insert_with_fallback(
    conn: asyncpg.Connection,
    table: str,
    columns: list[str],
    records: list[tuple[dict, int]],  # (record, row_number)
    version_id: int,
    batch_size: int = 1000,
) -> tuple[int, list[dict]]:
    """
    Batch insert records with fallback to individual inserts on error.

    Args:
        conn: Database connection
        table: Target table name
        columns: List of column names
        records: List of (record_dict, original_row_number) tuples
        version_id: Data version ID
        batch_size: Records per batch

    Returns:
        Tuple of (inserted_count, failed_rows)
        failed_rows is list of {row_number, record, error}
    """
    if not records:
        return 0, []

    all_columns = ["data_version_id"] + columns
    columns_str = ", ".join(all_columns)

    total_inserted = 0
    failed_rows = []

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        # Try batch insert first
        try:
            rows = []
            for record, _ in batch:
                row_values = [version_id]
                for col in columns:
                    row_values.append(record.get(col))
                rows.append(tuple(row_values))

            placeholders = ", ".join(
                f"({', '.join(f'${j}' for j in range(1 + idx * len(all_columns), 1 + (idx + 1) * len(all_columns)))})"
                for idx in range(len(batch))
            )

            flat_values = []
            for row in rows:
                flat_values.extend(row)

            query = f"INSERT INTO {table} ({columns_str}) VALUES {placeholders}"
            await conn.execute(query, *flat_values)
            total_inserted += len(batch)

        except Exception as batch_error:
            # Batch failed - fall back to individual inserts
            for record, row_number in batch:
                try:
                    row_values = [version_id]
                    for col in columns:
                        row_values.append(record.get(col))

                    placeholders = ", ".join(f"${i+1}" for i in range(len(all_columns)))
                    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
                    await conn.execute(query, *row_values)
                    total_inserted += 1

                except Exception as row_error:
                    failed_rows.append({
                        "row_number": row_number,
                        "record": record,
                        "error": str(row_error),
                    })

    return total_inserted, failed_rows


# ============================================================
# Duplicate Detection
# ============================================================


def detect_duplicates(
    records: list[tuple[dict, int]],  # (record, row_number)
    unique_keys: list[str],
) -> tuple[list[tuple[dict, int]], int, list[tuple[dict, int]]]:
    """
    Detect and remove duplicate records within the file.

    Returns:
        Tuple of (unique_records, duplicate_count, duplicate_records)
    """
    seen = set()
    unique_records = []
    duplicates = []

    for record, row_number in records:
        key = tuple(record.get(k) for k in unique_keys)

        # Skip if any key component is None (can't determine uniqueness)
        if None in key:
            unique_records.append((record, row_number))
            continue

        if key in seen:
            duplicates.append((record, row_number))
        else:
            seen.add(key)
            unique_records.append((record, row_number))

    return unique_records, len(duplicates), duplicates


# ============================================================
# Main Ingestion Function
# ============================================================


async def ingest_data(
    conn: asyncpg.Connection,
    source_code: str,
    df: pd.DataFrame,
    header_row_index: int,
    column_map: dict[str, str],
    version_id: int,
    batch_size: int = 1000,
) -> dict:
    """
    Ingest data from a DataFrame into the appropriate CMS table.
    Skips empty rows and handles individual row errors gracefully.

    Returns:
        Dict with ingestion results including failed_rows for UI display
    """
    config = TABLE_CONFIG.get(source_code)
    if not config:
        return {
            "success": False,
            "records_processed": 0,
            "records_inserted": 0,
            "duplicates_skipped": 0,
            "rows_skipped": 0,
            "errors": [f"Unknown source code: {source_code}"],
            "failed_rows": [],
            "column_stats": {},
        }

    table = config["table"]
    unique_keys = config["unique_keys"]
    columns = config["columns"]

    # Get data type map from database
    type_map = await _get_type_map(conn, source_code)

    # Build header index map
    header_values = [str(val).strip() for val in df.iloc[header_row_index].tolist()]
    header_to_idx = {val: idx for idx, val in enumerate(header_values)}

    # Track statistics
    data_start_row = header_row_index + 1
    records = []  # List of (record, row_number)
    column_stats = {col: {"null_count": 0, "sample_values": []} for col in columns}
    transform_errors = []
    rows_skipped = 0

    # Transform all records
    for row_idx in range(data_start_row, len(df)):
        row = df.iloc[row_idx]
        row_number = row_idx + 1  # 1-indexed for display

        # Skip empty/metadata rows
        if is_empty_row(row):
            rows_skipped += 1
            continue

        try:
            record = transform_record(row, column_map, type_map, header_to_idx, source_code)

            # Validate record
            is_valid, error_msg = validate_record(record, unique_keys, row_number)
            if not is_valid:
                transform_errors.append(error_msg)
                await log_ingestion_event(
                    conn, version_id, "WARNING", error_msg,
                    {"row_number": row_number, "record": {k: str(v)[:100] for k, v in record.items()}},
                )
                continue

            records.append((record, row_number))

            # Update column stats
            for col in columns:
                val = record.get(col)
                if val is None:
                    column_stats[col]["null_count"] += 1
                elif len(column_stats[col]["sample_values"]) < 3:
                    column_stats[col]["sample_values"].append(str(val)[:50])

        except Exception as e:
            transform_errors.append(f"Row {row_number}: {str(e)}")
            await log_ingestion_event(
                conn, version_id, "WARNING",
                f"Error transforming row {row_number}",
                {"error": str(e)},
            )

    records_processed = len(records)

    # Detect duplicates
    unique_records, duplicate_count, _ = detect_duplicates(records, unique_keys)

    if duplicate_count > 0:
        await log_ingestion_event(
            conn, version_id, "INFO",
            f"Found {duplicate_count} duplicate records in file",
            {"duplicate_count": duplicate_count, "unique_keys": unique_keys},
        )

    # Batch insert with fallback
    records_inserted, failed_rows = await batch_insert_with_fallback(
        conn, table, columns, unique_records, version_id, batch_size
    )

    # Log failed rows
    for failed in failed_rows:
        await log_ingestion_event(
            conn, version_id, "ERROR",
            f"Failed to insert row {failed['row_number']}: {failed['error']}",
            {"row_number": failed["row_number"], "error": failed["error"]},
        )

    # Calculate null percentages
    for col in columns:
        if records_processed > 0:
            column_stats[col]["null_percentage"] = round(
                100 * column_stats[col]["null_count"] / records_processed, 2
            )

    # Success if at least some records were inserted
    success = records_inserted > 0

    await log_ingestion_event(
        conn, version_id, "INFO",
        f"Ingestion complete: {records_inserted} records inserted, {len(failed_rows)} failed",
        {
            "records_processed": records_processed,
            "records_inserted": records_inserted,
            "duplicates_skipped": duplicate_count,
            "rows_skipped": rows_skipped,
            "failed_count": len(failed_rows),
        },
    )

    return {
        "success": success,
        "records_processed": records_processed,
        "records_inserted": records_inserted,
        "duplicates_skipped": duplicate_count,
        "rows_skipped": rows_skipped,
        "errors": transform_errors,
        "failed_rows": failed_rows,
        "column_stats": column_stats,
    }


async def _get_type_map(conn: asyncpg.Connection, source_code: str) -> dict[str, str]:
    """Get data type map from canonical_columns."""
    rows = await conn.fetch(
        """
        SELECT cc.internal_name, cc.data_type
        FROM meta.canonical_columns cc
        JOIN meta.data_sources ds ON cc.source_id = ds.id
        WHERE ds.source_code = $1
        """,
        source_code,
    )
    return {row["internal_name"]: row["data_type"] for row in rows}


# ============================================================
# Full Ingestion Flow
# ============================================================


async def delete_failed_version(
    conn: asyncpg.Connection,
    source_id: int,
    version_label: str,
    variant: Optional[str],
) -> bool:
    """
    Delete a failed version record to allow re-upload.
    Returns True if a record was deleted.
    """
    result = await conn.execute(
        """
        DELETE FROM meta.data_versions
        WHERE source_id = $1 AND version_label = $2 AND variant IS NOT DISTINCT FROM $3
          AND status = 'failed'
        """,
        source_id, version_label, variant,
    )
    return result == "DELETE 1"


async def ingest_file(
    conn: asyncpg.Connection,
    source_id: int,
    source_code: str,
    df: pd.DataFrame,
    header_row_index: int,
    column_map: dict[str, str],
    version_label: str,
    variant: Optional[str],
    effective_date: datetime,
    file_name: str,
    file_hash: str,
    file_size_bytes: int,
    mark_as_current: bool = False,
    append_to_existing: bool = False,
) -> dict:
    """
    Complete file ingestion flow with version management.
    Succeeds if any records are inserted (partial success allowed).

    For multi-part files (like NCCI PTP), set append_to_existing=True to
    add data to an existing version rather than creating a new one.
    """
    is_appending = False
    existing_version = None

    # Check if we should append to existing version (for multi-part files)
    if append_to_existing:
        existing_version = await get_existing_version(conn, source_id, version_label, variant)
        if existing_version:
            is_appending = True
            version_id = existing_version["id"]
            part_number = (existing_version.get("part_count") or 1) + 1
        else:
            # No existing completed version - check for failed version and clean up
            await delete_failed_version(conn, source_id, version_label, variant)
            # Create new one
            version_id = await create_data_version(
                conn, source_id, version_label, variant, effective_date,
                file_name, file_hash, file_size_bytes, header_row_index,
            )
    else:
        # Clean up any failed version with same key before creating new one
        await delete_failed_version(conn, source_id, version_label, variant)
        # Create new version record
        version_id = await create_data_version(
            conn, source_id, version_label, variant, effective_date,
            file_name, file_hash, file_size_bytes, header_row_index,
        )

    try:
        # Ingest data
        result = await ingest_data(
            conn, source_code, df, header_row_index, column_map, version_id
        )

        if result["records_inserted"] > 0:
            if is_appending:
                # Add part record and update version totals
                await add_version_part(
                    conn, version_id, part_number, file_name, file_hash,
                    file_size_bytes, result["records_inserted"],
                )
                await update_version_for_part(conn, version_id, result["records_inserted"])
                result["is_part"] = True
                result["part_number"] = part_number
            else:
                # Update version status to completed (even with partial success)
                await update_version_status(
                    conn, version_id, "completed",
                    record_count=result["records_inserted"],
                )

            # Mark as current if requested
            if mark_as_current:
                await mark_version_as_current(conn, source_id, version_id, variant)

            result["success"] = True
        else:
            if not is_appending:
                # No records inserted - mark as failed (only for new versions)
                error_msg = "; ".join(result["errors"][:5]) if result["errors"] else "No records could be inserted"
                await update_version_status(conn, version_id, "failed", error_message=error_msg)
            result["success"] = False

        result["version_id"] = version_id
        result["is_appending"] = is_appending
        return result

    except Exception as e:
        if not is_appending:
            await update_version_status(conn, version_id, "failed", error_message=str(e))
        raise
