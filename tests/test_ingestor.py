"""
Tests for ingestor service.
"""

import pytest
import pandas as pd
from datetime import date

from app.services.ingestor import (
    transform_record,
    detect_duplicates,
    TABLE_CONFIG,
)


class TestTableConfig:
    """Tests for TABLE_CONFIG structure."""

    def test_all_sources_have_config(self):
        """Test that all expected sources have configuration."""
        expected_sources = [
            "PFS_RVU", "PFS_GPCI", "PFS_LOCALITY", "PFS_ANES_CF",
            "PFS_OPPS_CAP", "HCPCS", "NCCI_PTP",
            "NCCI_MUE_DME", "NCCI_MUE_PRAC", "NCCI_MUE_OPH",
        ]
        for source in expected_sources:
            assert source in TABLE_CONFIG, f"Missing config for {source}"

    def test_config_has_required_keys(self):
        """Test that each config has required keys."""
        for source, config in TABLE_CONFIG.items():
            assert "table" in config, f"{source} missing 'table'"
            assert "unique_keys" in config, f"{source} missing 'unique_keys'"
            assert "columns" in config, f"{source} missing 'columns'"
            assert len(config["unique_keys"]) > 0, f"{source} has empty unique_keys"
            assert len(config["columns"]) > 0, f"{source} has empty columns"

    def test_unique_keys_in_columns(self):
        """Test that unique_keys are subset of columns."""
        for source, config in TABLE_CONFIG.items():
            for key in config["unique_keys"]:
                assert key in config["columns"], \
                    f"{source}: unique_key '{key}' not in columns"


class TestTransformRecord:
    """Tests for transform_record function."""

    def test_basic_pfs_rvu_transform(self):
        """Test transforming a PFS RVU record."""
        row = pd.Series(["99213", "", "Office visit", "A", "1.50", "2.00", "1.00", "0.10"])
        column_map = {
            "hcpcs_code": "HCPCS",
            "modifier": "Mod",
            "description": "Description",
            "status_code": "Status",
            "work_rvu": "Work RVU",
            "non_fac_pe_rvu": "Non-Fac PE RVU",
            "facility_pe_rvu": "Facility PE RVU",
            "mp_rvu": "MP RVU",
        }
        type_map = {
            "hcpcs_code": "TEXT",
            "modifier": "TEXT",
            "description": "TEXT",
            "status_code": "TEXT",
            "work_rvu": "NUMERIC",
            "non_fac_pe_rvu": "NUMERIC",
            "facility_pe_rvu": "NUMERIC",
            "mp_rvu": "NUMERIC",
        }
        header_to_idx = {
            "HCPCS": 0, "Mod": 1, "Description": 2, "Status": 3,
            "Work RVU": 4, "Non-Fac PE RVU": 5, "Facility PE RVU": 6, "MP RVU": 7,
        }

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["hcpcs_code"] == "99213"
        assert record["modifier"] is None  # Empty string becomes None
        assert record["description"] == "Office visit"
        assert record["work_rvu"] == 1.5
        assert record["mp_rvu"] == 0.1

    def test_mue_mai_id_extraction(self):
        """Test that MAI ID is extracted from MAI description."""
        row = pd.Series(["A1234", "5", "Line Edit", "1 Line Edit"])
        column_map = {
            "hcpcs_code": "HCPCS",
            "mue_value": "MUE Value",
            "mue_rationale": "Rationale",
            "mai_id": "MAI",  # Will be extracted from mai_description
            "mai_description": "MAI",
        }
        type_map = {
            "hcpcs_code": "TEXT",
            "mue_value": "INTEGER",
            "mue_rationale": "TEXT",
            "mai_id": "INTEGER",
            "mai_description": "TEXT",
        }
        header_to_idx = {"HCPCS": 0, "MUE Value": 1, "Rationale": 2, "MAI": 3}

        record = transform_record(row, column_map, type_map, header_to_idx, "NCCI_MUE_DME")

        assert record["hcpcs_code"] == "A1234"
        assert record["mue_value"] == 5
        assert record["mai_id"] == 1

    def test_mue_value_zero_preserved(self):
        """Test that MUE value of 0 is preserved."""
        row = pd.Series(["B1234", "0", "", "1 Line Edit"])
        column_map = {
            "hcpcs_code": "HCPCS",
            "mue_value": "MUE",
            "mue_rationale": "Rationale",
            "mai_description": "MAI",
        }
        type_map = {
            "hcpcs_code": "TEXT",
            "mue_value": "INTEGER",
            "mue_rationale": "TEXT",
            "mai_description": "TEXT",
        }
        header_to_idx = {"HCPCS": 0, "MUE": 1, "Rationale": 2, "MAI": 3}

        record = transform_record(row, column_map, type_map, header_to_idx, "NCCI_MUE_PRAC")

        assert record["mue_value"] == 0  # Not None

    def test_ncci_ptp_special_fields(self):
        """Test NCCI PTP special field handling."""
        row = pd.Series(["00100", "00101", "1", "20240101", "*", "Some rationale", "*"])
        column_map = {
            "comprehensive_code": "Col1",
            "component_code": "Col2",
            "modifier_indicator": "Modifier",
            "effective_date": "Effective Date",
            "deletion_date": "Deletion Date",
            "rationale": "Rationale",
            "prior_1996_flag": "Prior Flag",
        }
        type_map = {
            "comprehensive_code": "TEXT",
            "component_code": "TEXT",
            "modifier_indicator": "INTEGER",
            "effective_date": "DATE",
            "deletion_date": "DATE",
            "rationale": "TEXT",
            "prior_1996_flag": "BOOLEAN",
        }
        header_to_idx = {
            "Col1": 0, "Col2": 1, "Modifier": 2, "Effective Date": 3,
            "Deletion Date": 4, "Rationale": 5, "Prior Flag": 6,
        }

        record = transform_record(row, column_map, type_map, header_to_idx, "NCCI_PTP")

        assert record["comprehensive_code"] == "00100"
        assert record["component_code"] == "00101"
        assert record["modifier_indicator"] == 1
        assert record["effective_date"] == date(2024, 1, 1)
        assert record["deletion_date"] is None  # * means active
        assert record["prior_1996_flag"] is True  # * means prior to 1996

    def test_code_columns_uppercased(self):
        """Test that code columns are uppercased."""
        row = pd.Series(["a1234", "Office visit", "1.00"])
        column_map = {
            "hcpcs_code": "Code",
            "description": "Desc",
            "amount": "Amount",
        }
        type_map = {"hcpcs_code": "TEXT", "description": "TEXT", "amount": "NUMERIC"}
        header_to_idx = {"Code": 0, "Desc": 1, "Amount": 2}

        record = transform_record(row, column_map, type_map, header_to_idx, "HCPCS")

        assert record["hcpcs_code"] == "A1234"


class TestDetectDuplicates:
    """Tests for detect_duplicates function."""

    def test_no_duplicates(self):
        """Test with no duplicate records."""
        # Now expects list of (record, row_number) tuples
        records = [
            ({"hcpcs_code": "99213", "modifier": ""}, 1),
            ({"hcpcs_code": "99214", "modifier": ""}, 2),
            ({"hcpcs_code": "99215", "modifier": ""}, 3),
        ]
        unique_keys = ["hcpcs_code", "modifier"]

        unique, dup_count, dups = detect_duplicates(records, unique_keys)

        assert len(unique) == 3
        assert dup_count == 0
        assert len(dups) == 0

    def test_with_duplicates(self):
        """Test with duplicate records."""
        records = [
            ({"hcpcs_code": "99213", "modifier": ""}, 1),
            ({"hcpcs_code": "99214", "modifier": ""}, 2),
            ({"hcpcs_code": "99213", "modifier": ""}, 3),  # Duplicate
            ({"hcpcs_code": "99215", "modifier": ""}, 4),
            ({"hcpcs_code": "99213", "modifier": ""}, 5),  # Duplicate
        ]
        unique_keys = ["hcpcs_code", "modifier"]

        unique, dup_count, dups = detect_duplicates(records, unique_keys)

        assert len(unique) == 3
        assert dup_count == 2
        assert len(dups) == 2

    def test_multikey_duplicates(self):
        """Test duplicates with multiple key columns."""
        records = [
            ({"hcpcs_code": "99213", "modifier": ""}, 1),
            ({"hcpcs_code": "99213", "modifier": "26"}, 2),  # Not a duplicate
            ({"hcpcs_code": "99213", "modifier": ""}, 3),   # Duplicate
        ]
        unique_keys = ["hcpcs_code", "modifier"]

        unique, dup_count, dups = detect_duplicates(records, unique_keys)

        assert len(unique) == 2
        assert dup_count == 1

    def test_null_key_not_considered_duplicate(self):
        """Test that records with null keys are kept."""
        records = [
            ({"hcpcs_code": "99213", "modifier": ""}, 1),
            ({"hcpcs_code": None, "modifier": ""}, 2),  # Null key - kept
            ({"hcpcs_code": "99213", "modifier": ""}, 3),  # Duplicate
        ]
        unique_keys = ["hcpcs_code", "modifier"]

        unique, dup_count, dups = detect_duplicates(records, unique_keys)

        # First and second kept (second has null), third is duplicate
        assert len(unique) == 2
        assert dup_count == 1

    def test_preserves_order(self):
        """Test that first occurrence is kept, duplicates removed."""
        records = [
            ({"code": "A", "value": 1}, 1),
            ({"code": "B", "value": 2}, 2),
            ({"code": "A", "value": 3}, 3),  # Duplicate - should be removed
        ]
        unique_keys = ["code"]

        unique, _, _ = detect_duplicates(records, unique_keys)

        assert len(unique) == 2
        assert unique[0][0]["value"] == 1  # First A kept (tuple format: record at index 0)
        assert unique[1][0]["value"] == 2  # B kept


class TestDataIntegrity:
    """Tests for data integrity in transformations."""

    def test_leading_zeros_preserved_in_codes(self):
        """Test that leading zeros are preserved in code values."""
        row = pd.Series(["00100", "Description"])
        column_map = {"hcpcs_code": "Code", "description": "Desc"}
        type_map = {"hcpcs_code": "TEXT", "description": "TEXT"}
        header_to_idx = {"Code": 0, "Desc": 1}

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["hcpcs_code"] == "00100"

    def test_empty_string_becomes_none(self):
        """Test that empty strings become None for optional fields."""
        row = pd.Series(["99213", "", ""])
        column_map = {"hcpcs_code": "Code", "modifier": "Mod", "description": "Desc"}
        type_map = {"hcpcs_code": "TEXT", "modifier": "TEXT", "description": "TEXT"}
        header_to_idx = {"Code": 0, "Mod": 1, "Desc": 2}

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["hcpcs_code"] == "99213"
        assert record["modifier"] is None
        assert record["description"] is None

    def test_numeric_with_commas(self):
        """Test that numeric values with commas are parsed correctly."""
        row = pd.Series(["99213", "1,234.56"])
        column_map = {"hcpcs_code": "Code", "amount": "Amount"}
        type_map = {"hcpcs_code": "TEXT", "amount": "NUMERIC"}
        header_to_idx = {"Code": 0, "Amount": 1}

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["amount"] == 1234.56

    def test_date_formats(self):
        """Test various date format parsing."""
        # YYYYMMDD format (common in CMS files)
        row = pd.Series(["A1234", "20240115"])
        column_map = {"hcpcs_code": "Code", "effective_date": "Date"}
        type_map = {"hcpcs_code": "TEXT", "effective_date": "DATE"}
        header_to_idx = {"Code": 0, "Date": 1}

        record = transform_record(row, column_map, type_map, header_to_idx, "HCPCS")

        assert record["effective_date"] == date(2024, 1, 15)


class TestEdgeCases:
    """Tests for edge cases in ingestion."""

    def test_missing_column_in_header(self):
        """Test handling of columns not in file."""
        row = pd.Series(["99213", "1.50"])
        column_map = {
            "hcpcs_code": "Code",
            "work_rvu": "Work RVU",
            "mp_rvu": "MP RVU",  # Not in header_to_idx
        }
        type_map = {"hcpcs_code": "TEXT", "work_rvu": "NUMERIC", "mp_rvu": "NUMERIC"}
        header_to_idx = {"Code": 0, "Work RVU": 1}  # MP RVU missing

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["hcpcs_code"] == "99213"
        assert record["work_rvu"] == 1.5
        assert "mp_rvu" not in record  # Not added because not in header

    def test_whitespace_handling(self):
        """Test that whitespace is properly handled."""
        row = pd.Series(["  99213  ", "  1.50  ", "  Office visit  "])
        column_map = {"hcpcs_code": "Code", "work_rvu": "RVU", "description": "Desc"}
        type_map = {"hcpcs_code": "TEXT", "work_rvu": "NUMERIC", "description": "TEXT"}
        header_to_idx = {"Code": 0, "RVU": 1, "Desc": 2}

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["hcpcs_code"] == "99213"
        assert record["work_rvu"] == 1.5
        assert record["description"] == "Office visit"

    def test_null_and_na_values(self):
        """Test that NULL and N/A values become None."""
        row = pd.Series(["99213", "NULL", "N/A", "*"])
        column_map = {
            "hcpcs_code": "Code",
            "work_rvu": "RVU1",
            "mp_rvu": "RVU2",
            "description": "Desc",
        }
        type_map = {
            "hcpcs_code": "TEXT",
            "work_rvu": "NUMERIC",
            "mp_rvu": "NUMERIC",
            "description": "TEXT",
        }
        header_to_idx = {"Code": 0, "RVU1": 1, "RVU2": 2, "Desc": 3}

        record = transform_record(row, column_map, type_map, header_to_idx, "PFS_RVU")

        assert record["work_rvu"] is None
        assert record["mp_rvu"] is None
        # Note: "*" in description remains as "*" since it's TEXT
