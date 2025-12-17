"""
Tests for header_detector service.
"""

import pytest
import pandas as pd

from app.services.header_detector import (
    detect_header_row,
    get_column_index,
)


class TestDetectHeaderRow:
    """Tests for detect_header_row function."""

    def test_header_on_first_row(self):
        """Test detection when header is on first row."""
        df = pd.DataFrame([
            ["HCPCS", "WORK RVU", "DESCRIPTION"],
            ["99213", "1.50", "Office visit"],
            ["99214", "2.00", "Office visit complex"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS", "HCPC"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},
            "description": {"headers": ["DESCRIPTION"], "is_required": False},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert result["header_row_index"] == 0
        assert "hcpcs_code" in result["column_map"]
        assert "work_rvu" in result["column_map"]
        assert result["column_map"]["hcpcs_code"] == "HCPCS"
        assert result["error"] is None

    def test_header_after_metadata_rows(self):
        """Test detection when header is after metadata/copyright rows."""
        df = pd.DataFrame([
            ["CMS Physician Fee Schedule", "", ""],
            ["Copyright 2024", "", ""],
            ["", "", ""],
            ["HCPCS", "WORK RVU", "DESCRIPTION"],
            ["99213", "1.50", "Office visit"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert result["header_row_index"] == 3

    def test_case_insensitive_matching(self):
        """Test that header matching is case-insensitive."""
        df = pd.DataFrame([
            ["hcpcs", "Work Rvu", "DESCRIPTION"],
            ["99213", "1.50", "Office visit"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert result["header_row_index"] == 0

    def test_missing_required_column(self):
        """Test that missing required columns result in not found."""
        df = pd.DataFrame([
            ["HCPCS", "DESCRIPTION"],
            ["99213", "Office visit"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},  # Missing!
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is False
        assert "work_rvu" in result["error"]

    def test_optional_column_missing_ok(self):
        """Test that missing optional columns don't prevent detection."""
        df = pd.DataFrame([
            ["HCPCS", "WORK RVU"],
            ["99213", "1.50"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},
            "description": {"headers": ["DESCRIPTION"], "is_required": False},  # Optional
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert "description" not in result["column_map"]

    def test_unmapped_columns_tracked(self):
        """Test that unmapped columns are tracked."""
        df = pd.DataFrame([
            ["HCPCS", "WORK RVU", "EXTRA_COL", "ANOTHER"],
            ["99213", "1.50", "foo", "bar"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU"], "is_required": True},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert "EXTRA_COL" in result["unmapped_columns"]
        assert "ANOTHER" in result["unmapped_columns"]

    def test_partial_match_long_header(self):
        """Test partial matching for long headers like NCCI files."""
        df = pd.DataFrame([
            ["Column 1", "Column 2", "Modifier 0=not allowed 1=allowed 9=N/A"],
            ["00100", "00101", "1"],
        ])

        column_mappings = {
            "comprehensive_code": {"headers": ["Column 1"], "is_required": True},
            "component_code": {"headers": ["Column 2"], "is_required": True},
            "modifier_indicator": {"headers": ["Modifier"], "is_required": True},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert "modifier_indicator" in result["column_map"]

    def test_multiple_header_variations(self):
        """Test that multiple header variations are checked."""
        df = pd.DataFrame([
            ["HCPC", "WRVU", "DESC"],  # Alternative names
            ["99213", "1.50", "Office visit"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS", "HCPC", "CPT"], "is_required": True},
            "work_rvu": {"headers": ["WORK RVU", "WRVU"], "is_required": True},
            "description": {"headers": ["DESCRIPTION", "DESC"], "is_required": False},
        }

        result = detect_header_row(df, column_mappings)

        assert result["found"] is True
        assert result["column_map"]["hcpcs_code"] == "HCPC"
        assert result["column_map"]["work_rvu"] == "WRVU"

    def test_no_header_found_in_scan_range(self):
        """Test error when no header found within scan range."""
        # Create DataFrame with all data rows
        df = pd.DataFrame([
            ["99213", "1.50", "Office visit"],
            ["99214", "2.00", "Office visit complex"],
        ])

        column_mappings = {
            "hcpcs_code": {"headers": ["HCPCS"], "is_required": True},
        }

        result = detect_header_row(df, column_mappings, max_scan_rows=2)

        assert result["found"] is False
        assert "Could not find header row" in result["error"]


class TestGetColumnIndex:
    """Tests for get_column_index function."""

    def test_basic_index_mapping(self):
        """Test getting column indices."""
        df = pd.DataFrame([
            ["HCPCS", "WORK RVU", "DESCRIPTION"],
            ["99213", "1.50", "Office visit"],
        ])

        column_map = {
            "hcpcs_code": "HCPCS",
            "work_rvu": "WORK RVU",
            "description": "DESCRIPTION",
        }

        indices = get_column_index(df, 0, column_map)

        assert indices["hcpcs_code"] == 0
        assert indices["work_rvu"] == 1
        assert indices["description"] == 2

    def test_missing_header_excluded(self):
        """Test that missing headers are excluded from index mapping."""
        df = pd.DataFrame([
            ["HCPCS", "WORK RVU"],
            ["99213", "1.50"],
        ])

        column_map = {
            "hcpcs_code": "HCPCS",
            "description": "DESCRIPTION",  # Not in DataFrame
        }

        indices = get_column_index(df, 0, column_map)

        assert "hcpcs_code" in indices
        assert "description" not in indices
