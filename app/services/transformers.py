"""
Data transformation utilities.
Handles type conversion, date parsing, and special value handling.
"""

import re
from datetime import datetime, date
from typing import Optional, Any


def parse_date(value: str, formats: list[str] | None = None) -> Optional[date]:
    """
    Parse a date string into a date object.

    Handles:
    - YYYYMMDD (CMS NCCI format)
    - MM/DD/YYYY
    - YYYY-MM-DD
    - '*' or empty -> None

    Args:
        value: String value to parse
        formats: Optional list of format strings to try

    Returns:
        date object or None
    """
    if not value or str(value).strip() in ["*", "", "NULL", "N/A", "nan", "NaN"]:
        return None

    value = str(value).strip()

    # Default formats to try
    if formats is None:
        formats = ["%Y%m%d", "%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d", "%m-%d-%Y"]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    return None


def parse_numeric(value: str) -> Optional[float]:
    """
    Parse a numeric string.

    Handles:
    - Comma-separated numbers (1,234.56)
    - Empty strings -> None
    - Non-numeric -> None

    Args:
        value: String value to parse

    Returns:
        float or None
    """
    if value is None:
        return None

    value_str = str(value).strip()

    if value_str in ["", "NULL", "N/A", "*", "nan", "NaN"]:
        return None

    # Remove commas
    value_str = value_str.replace(",", "")

    try:
        return float(value_str)
    except ValueError:
        return None


def parse_integer(value: str) -> Optional[int]:
    """
    Parse an integer string.

    Args:
        value: String value to parse

    Returns:
        int or None
    """
    num = parse_numeric(value)
    if num is not None:
        return int(num)
    return None


def parse_boolean(value: str) -> Optional[bool]:
    """
    Parse a boolean string.

    Handles:
    - '1', 'true', 'yes', 'y', '*' -> True
    - '0', 'false', 'no', 'n', '' -> False

    Args:
        value: String value to parse

    Returns:
        bool or None
    """
    if value is None:
        return None

    value_str = str(value).strip().lower()

    if value_str in ["1", "true", "yes", "y", "*"]:
        return True
    elif value_str in ["0", "false", "no", "n", ""]:
        return False

    return None


def clean_code(value: str) -> Optional[str]:
    """
    Clean a code value (HCPCS, CPT, etc.).
    Preserves leading zeros and alphanumeric characters.

    Args:
        value: Code string

    Returns:
        Cleaned code string, uppercase, or None if empty
    """
    if value is None:
        return None

    cleaned = str(value).strip().upper()

    if cleaned in ["", "NULL", "N/A", "nan", "NaN"]:
        return None

    return cleaned


def transform_value(value: str, data_type: str) -> Any:
    """
    Transform a string value based on expected data type.

    Args:
        value: String value from file
        data_type: Expected type (TEXT, NUMERIC, INTEGER, DATE, BOOLEAN)

    Returns:
        Transformed value
    """
    if data_type == "TEXT":
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned and cleaned not in ["NULL", "N/A", "nan", "NaN"] else None
    elif data_type == "NUMERIC":
        return parse_numeric(value)
    elif data_type == "INTEGER":
        return parse_integer(value)
    elif data_type == "DATE":
        return parse_date(value)
    elif data_type == "BOOLEAN":
        return parse_boolean(value)
    else:
        # Default to TEXT behavior
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned else None


# ============================================================
# MUE-specific transformers
# ============================================================


def parse_mai_id(value: str) -> Optional[int]:
    """
    Extract the MAI ID (1, 2, or 3) from the full description string.

    Examples:
        "1 Line Edit" -> 1
        "2 Date of Service Edit: Policy" -> 2
        "3 Date of Service Edit: Clinical" -> 3
        "" or None -> None

    Args:
        value: Full MAI description string

    Returns:
        Integer 1, 2, or 3, or None if not parseable
    """
    if not value or not str(value).strip():
        return None

    # Extract leading digit(s)
    match = re.match(r"^(\d+)", str(value).strip())
    if match:
        mai = int(match.group(1))
        # Validate it's a known MAI value
        if mai in [1, 2, 3]:
            return mai

    return None


def parse_mue_value(value: str) -> Optional[int]:
    """
    Parse MUE value, preserving zero as a valid value.

    Important: Zero is a valid MUE value meaning "not payable for this provider type."

    Args:
        value: String value from file

    Returns:
        Integer or None (but 0 returns 0, not None)
    """
    if value is None:
        return None

    value_str = str(value).strip()

    if value_str == "" or value_str.upper() in ["NULL", "N/A", "nan", "NaN"]:
        return None

    try:
        return int(float(value_str))
    except ValueError:
        return None


# ============================================================
# NCCI PTP-specific transformers
# ============================================================


def parse_deletion_date(value: str) -> Optional[date]:
    """
    Parse deletion date from NCCI PTP files.

    Special handling: '*' means currently active (return None).

    Args:
        value: Date string or '*'

    Returns:
        date object or None
    """
    if not value or str(value).strip() == "*":
        return None

    return parse_date(value)


def parse_modifier_indicator(value: str) -> Optional[int]:
    """
    Parse modifier indicator from NCCI PTP files.

    Values: 0, 1, or 9

    Args:
        value: Modifier indicator string

    Returns:
        Integer 0, 1, or 9, or None
    """
    if value is None:
        return None

    value_str = str(value).strip()

    # Handle case where header text is included
    # e.g., "0" or "1" extracted from "Modifier 0=not allowed..."
    if value_str and value_str[0].isdigit():
        try:
            indicator = int(value_str[0])
            if indicator in [0, 1, 9]:
                return indicator
        except ValueError:
            pass

    return parse_integer(value)


def parse_prior_1996_flag(value: str) -> Optional[bool]:
    """
    Parse the prior-to-1996 flag from NCCI PTP files.

    '*' in this column means True (existed before 1996).

    Args:
        value: Flag value, typically '*' or empty

    Returns:
        True if '*', False otherwise
    """
    if value is None:
        return None

    value_str = str(value).strip()

    return value_str == "*"
