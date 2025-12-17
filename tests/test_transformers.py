"""
Tests for transformers service.
"""

import pytest
from datetime import date

from app.services.transformers import (
    parse_date,
    parse_numeric,
    parse_integer,
    parse_boolean,
    clean_code,
    transform_value,
    parse_mai_id,
    parse_mue_value,
    parse_deletion_date,
    parse_modifier_indicator,
    parse_prior_1996_flag,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_yyyymmdd_format(self):
        """Test YYYYMMDD format (NCCI files)."""
        assert parse_date("20240101") == date(2024, 1, 1)
        assert parse_date("20231215") == date(2023, 12, 15)

    def test_mm_dd_yyyy_format(self):
        """Test MM/DD/YYYY format."""
        assert parse_date("01/15/2024") == date(2024, 1, 15)
        assert parse_date("12/31/2023") == date(2023, 12, 31)

    def test_iso_format(self):
        """Test YYYY-MM-DD format."""
        assert parse_date("2024-01-15") == date(2024, 1, 15)

    def test_empty_values_return_none(self):
        """Test that empty/null values return None."""
        assert parse_date("") is None
        assert parse_date("*") is None
        assert parse_date("NULL") is None
        assert parse_date("N/A") is None
        assert parse_date(None) is None

    def test_invalid_date_returns_none(self):
        """Test that invalid dates return None."""
        assert parse_date("not a date") is None
        assert parse_date("13/45/2024") is None


class TestParseNumeric:
    """Tests for parse_numeric function."""

    def test_integer_string(self):
        """Test parsing integer strings."""
        assert parse_numeric("123") == 123.0
        assert parse_numeric("0") == 0.0

    def test_float_string(self):
        """Test parsing float strings."""
        assert parse_numeric("123.45") == 123.45
        assert parse_numeric("0.5") == 0.5

    def test_comma_separated(self):
        """Test parsing comma-separated numbers."""
        assert parse_numeric("1,234") == 1234.0
        assert parse_numeric("1,234,567.89") == 1234567.89

    def test_empty_values_return_none(self):
        """Test that empty/null values return None."""
        assert parse_numeric("") is None
        assert parse_numeric("*") is None
        assert parse_numeric("NULL") is None
        assert parse_numeric("N/A") is None
        assert parse_numeric(None) is None

    def test_invalid_numeric_returns_none(self):
        """Test that invalid numerics return None."""
        assert parse_numeric("abc") is None
        assert parse_numeric("12.34.56") is None

    def test_whitespace_handling(self):
        """Test that whitespace is handled."""
        assert parse_numeric("  123  ") == 123.0
        assert parse_numeric(" 45.67 ") == 45.67


class TestParseInteger:
    """Tests for parse_integer function."""

    def test_integer_string(self):
        """Test parsing integer strings."""
        assert parse_integer("123") == 123
        assert parse_integer("0") == 0

    def test_float_truncated(self):
        """Test that floats are truncated to integers."""
        assert parse_integer("123.7") == 123
        assert parse_integer("99.1") == 99

    def test_empty_returns_none(self):
        """Test that empty values return None."""
        assert parse_integer("") is None
        assert parse_integer(None) is None


class TestParseBoolean:
    """Tests for parse_boolean function."""

    def test_true_values(self):
        """Test values that should parse as True."""
        assert parse_boolean("1") is True
        assert parse_boolean("true") is True
        assert parse_boolean("TRUE") is True
        assert parse_boolean("yes") is True
        assert parse_boolean("y") is True
        assert parse_boolean("*") is True

    def test_false_values(self):
        """Test values that should parse as False."""
        assert parse_boolean("0") is False
        assert parse_boolean("false") is False
        assert parse_boolean("FALSE") is False
        assert parse_boolean("no") is False
        assert parse_boolean("n") is False
        assert parse_boolean("") is False

    def test_none_for_invalid(self):
        """Test that invalid values return None."""
        assert parse_boolean("maybe") is None
        assert parse_boolean(None) is None


class TestCleanCode:
    """Tests for clean_code function."""

    def test_uppercase_conversion(self):
        """Test that codes are uppercased."""
        assert clean_code("a1234") == "A1234"
        assert clean_code("j9999") == "J9999"

    def test_whitespace_stripped(self):
        """Test that whitespace is stripped."""
        assert clean_code("  99213  ") == "99213"

    def test_preserves_leading_zeros(self):
        """Test that leading zeros are preserved."""
        assert clean_code("00100") == "00100"
        assert clean_code("01999") == "01999"

    def test_empty_returns_none(self):
        """Test that empty values return None."""
        assert clean_code("") is None
        assert clean_code("NULL") is None
        assert clean_code(None) is None


class TestTransformValue:
    """Tests for transform_value function."""

    def test_text_type(self):
        """Test TEXT type transformation."""
        assert transform_value("hello", "TEXT") == "hello"
        assert transform_value("  trimmed  ", "TEXT") == "trimmed"
        assert transform_value("", "TEXT") is None

    def test_numeric_type(self):
        """Test NUMERIC type transformation."""
        assert transform_value("123.45", "NUMERIC") == 123.45
        assert transform_value("invalid", "NUMERIC") is None

    def test_integer_type(self):
        """Test INTEGER type transformation."""
        assert transform_value("123", "INTEGER") == 123
        assert transform_value("123.7", "INTEGER") == 123

    def test_date_type(self):
        """Test DATE type transformation."""
        assert transform_value("20240101", "DATE") == date(2024, 1, 1)
        assert transform_value("invalid", "DATE") is None

    def test_boolean_type(self):
        """Test BOOLEAN type transformation."""
        assert transform_value("1", "BOOLEAN") is True
        assert transform_value("0", "BOOLEAN") is False


class TestParseMaiId:
    """Tests for parse_mai_id function (MUE files)."""

    def test_extract_mai_id(self):
        """Test extracting MAI ID from description."""
        assert parse_mai_id("1 Line Edit") == 1
        assert parse_mai_id("2 Date of Service Edit: Policy") == 2
        assert parse_mai_id("3 Date of Service Edit: Clinical") == 3

    def test_empty_returns_none(self):
        """Test that empty values return None."""
        assert parse_mai_id("") is None
        assert parse_mai_id(None) is None

    def test_invalid_mai_returns_none(self):
        """Test that invalid MAI values return None."""
        assert parse_mai_id("4 Unknown") is None  # 4 is not valid
        assert parse_mai_id("Not a number") is None


class TestParseMueValue:
    """Tests for parse_mue_value function."""

    def test_zero_is_valid(self):
        """Test that zero is preserved as valid MUE value."""
        assert parse_mue_value("0") == 0

    def test_positive_values(self):
        """Test positive MUE values."""
        assert parse_mue_value("1") == 1
        assert parse_mue_value("10") == 10
        assert parse_mue_value("999") == 999

    def test_empty_returns_none(self):
        """Test that empty values return None."""
        assert parse_mue_value("") is None
        assert parse_mue_value(None) is None


class TestParseDeletionDate:
    """Tests for parse_deletion_date function (NCCI PTP files)."""

    def test_asterisk_means_active(self):
        """Test that * returns None (currently active)."""
        assert parse_deletion_date("*") is None

    def test_valid_date(self):
        """Test parsing a valid deletion date."""
        assert parse_deletion_date("20240101") == date(2024, 1, 1)

    def test_empty_returns_none(self):
        """Test that empty values return None."""
        assert parse_deletion_date("") is None


class TestParseModifierIndicator:
    """Tests for parse_modifier_indicator function (NCCI PTP files)."""

    def test_valid_indicators(self):
        """Test valid modifier indicators."""
        assert parse_modifier_indicator("0") == 0
        assert parse_modifier_indicator("1") == 1
        assert parse_modifier_indicator("9") == 9

    def test_invalid_returns_none(self):
        """Test that invalid values return None."""
        assert parse_modifier_indicator("invalid") is None


class TestParsePrior1996Flag:
    """Tests for parse_prior_1996_flag function (NCCI PTP files)."""

    def test_asterisk_means_true(self):
        """Test that * means True."""
        assert parse_prior_1996_flag("*") is True

    def test_empty_means_false(self):
        """Test that empty means False."""
        assert parse_prior_1996_flag("") is False

    def test_none_returns_none(self):
        """Test that None returns None."""
        assert parse_prior_1996_flag(None) is None
