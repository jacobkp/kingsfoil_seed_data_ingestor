"""
Dynamic header row detection.
Scans first N rows to find the row containing expected column headers.
"""

import pandas as pd
from typing import Optional


def detect_header_row(
    df: pd.DataFrame,
    column_mappings: dict,
    max_scan_rows: int = 15,
) -> dict:
    """
    Detect which row contains the header.

    Algorithm:
    1. For each row in the first max_scan_rows rows
    2. Try to match cell values against known headers from column_mappings
    3. If all required columns are found, this is the header row
    4. Return the row index and the mapping of internal_name -> file_header

    Args:
        df: DataFrame with no header set (read with header=None)
        column_mappings: Dict of internal_name -> {headers: [...], is_required: bool}
        max_scan_rows: Maximum rows to scan (default 15)

    Returns:
        {
            'found': bool,
            'header_row_index': int or None,
            'column_map': {internal_name: file_header},
            'unmapped_columns': [file_headers not mapped],
            'error': str or None
        }
    """
    rows_to_scan = min(max_scan_rows, len(df))

    for row_idx in range(rows_to_scan):
        # Get row values as uppercase strings for matching
        row_values = [str(val).strip().upper() for val in df.iloc[row_idx].tolist()]
        # Keep original values for output
        row_values_original = [str(val).strip() for val in df.iloc[row_idx].tolist()]

        # Try to match headers
        column_map = {}
        matched_indices = set()

        for internal_name, mapping_info in column_mappings.items():
            known_headers = [h.upper() for h in mapping_info["headers"]]

            for col_idx, cell_value in enumerate(row_values):
                if col_idx in matched_indices:
                    continue

                # Skip empty cells
                if not cell_value:
                    continue

                # Check for exact match
                if cell_value in known_headers:
                    column_map[internal_name] = row_values_original[col_idx]
                    matched_indices.add(col_idx)
                    break

                # Check for partial match (for long headers like "Modifier 0=not allowed...")
                for known in known_headers:
                    if _is_partial_match(cell_value, known):
                        column_map[internal_name] = row_values_original[col_idx]
                        matched_indices.add(col_idx)
                        break

                if internal_name in column_map:
                    break

        # Check if all required columns were found
        required_found = all(
            internal_name in column_map
            for internal_name, info in column_mappings.items()
            if info["is_required"]
        )

        # Need at least some columns matched to consider this a header row
        if required_found and len(column_map) > 0:
            # Find unmapped columns (non-empty cells that weren't matched)
            unmapped = [
                row_values_original[i]
                for i in range(len(row_values))
                if i not in matched_indices and row_values[i]
            ]

            return {
                "found": True,
                "header_row_index": row_idx,
                "column_map": column_map,
                "unmapped_columns": unmapped,
                "error": None,
            }

    # No header row found - build helpful error message
    required_cols = [
        name for name, info in column_mappings.items() if info["is_required"]
    ]

    return {
        "found": False,
        "header_row_index": None,
        "column_map": {},
        "unmapped_columns": [],
        "error": (
            f"Could not find header row in first {rows_to_scan} rows. "
            f"Missing required columns: {', '.join(required_cols)}"
        ),
    }


def _is_partial_match(cell_value: str, known_header: str) -> bool:
    """
    Check if cell_value partially matches known_header.

    Handles cases like:
    - "Modifier 0=not allowed 1=allowed..." matching "Modifier"
    - "WORK RVU" matching "WORK RVU (some extra text)"

    Args:
        cell_value: Uppercase cell value from file
        known_header: Uppercase known header from mappings

    Returns:
        True if partial match, False otherwise
    """
    # Cell starts with known header
    if cell_value.startswith(known_header):
        return True

    # Known header starts with cell (cell is abbreviated)
    if known_header.startswith(cell_value) and len(cell_value) >= 3:
        return True

    return False


def get_column_index(
    df: pd.DataFrame,
    header_row_index: int,
    column_map: dict,
) -> dict[str, int]:
    """
    Get column indices for mapped columns.

    Args:
        df: DataFrame
        header_row_index: Row index where headers are
        column_map: Mapping of internal_name -> file_header

    Returns:
        Dict of internal_name -> column_index
    """
    header_values = [str(val).strip() for val in df.iloc[header_row_index].tolist()]
    header_to_idx = {val: idx for idx, val in enumerate(header_values)}

    return {
        internal_name: header_to_idx[file_header]
        for internal_name, file_header in column_map.items()
        if file_header in header_to_idx
    }
