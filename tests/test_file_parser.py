"""
Tests for file_parser service.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from app.services.file_parser import (
    parse_file,
    get_row_as_list,
    get_file_extension,
)


class TestParseFile:
    """Tests for parse_file function."""

    def test_parse_csv_basic(self):
        """Test parsing a basic CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2,col3\n")
            f.write("a,b,c\n")
            f.write("1,2,3\n")
            f.name

        try:
            df, ext = parse_file(f.name)

            assert ext == ".csv"
            assert len(df) == 3
            assert len(df.columns) == 3
            # First row should be the header (since we read with header=None)
            assert df.iloc[0, 0] == "col1"
            assert df.iloc[1, 0] == "a"
        finally:
            os.unlink(f.name)

    def test_parse_csv_with_commas_in_values(self):
        """Test parsing CSV with quoted values containing commas."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write('name,description\n')
            f.write('"Smith, John","A description, with commas"\n')
            f.name

        try:
            df, ext = parse_file(f.name)

            assert len(df) == 2
            assert df.iloc[1, 0] == "Smith, John"
            assert df.iloc[1, 1] == "A description, with commas"
        finally:
            os.unlink(f.name)

    def test_parse_txt_tab_delimited(self):
        """Test parsing a tab-delimited TXT file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("col1\tcol2\tcol3\n")
            f.write("a\tb\tc\n")
            f.name

        try:
            df, ext = parse_file(f.name)

            assert ext == ".txt"
            assert len(df) == 2
            assert len(df.columns) == 3
        finally:
            os.unlink(f.name)

    def test_parse_file_not_found(self):
        """Test that FileNotFoundError is raised for missing files."""
        with pytest.raises(FileNotFoundError):
            parse_file("/nonexistent/path/file.csv")

    def test_parse_unsupported_extension(self):
        """Test that ValueError is raised for unsupported file types."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"key": "value"}')
            f.name

        try:
            with pytest.raises(ValueError, match="Unsupported file type"):
                parse_file(f.name)
        finally:
            os.unlink(f.name)

    def test_all_values_as_strings(self):
        """Test that all values are read as strings."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("num,float,text\n")
            f.write("123,45.67,hello\n")
            f.write("000,0.00,world\n")  # Leading zeros should be preserved
            f.name

        try:
            df, _ = parse_file(f.name)

            # All values should be strings
            assert df.iloc[1, 0] == "123"
            assert df.iloc[1, 1] == "45.67"
            # Leading zeros preserved
            assert df.iloc[2, 0] == "000"
        finally:
            os.unlink(f.name)

    def test_empty_values_not_converted_to_nan(self):
        """Test that empty values remain as empty strings, not NaN."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2,col3\n")
            f.write("a,,c\n")
            f.write(",b,\n")
            f.name

        try:
            df, _ = parse_file(f.name)

            # Empty values should be empty strings, not NaN
            assert df.iloc[1, 1] == ""
            assert df.iloc[2, 0] == ""
            assert df.iloc[2, 2] == ""
        finally:
            os.unlink(f.name)


class TestGetRowAsList:
    """Tests for get_row_as_list function."""

    def test_basic_row(self):
        """Test getting a basic row."""
        df = pd.DataFrame([["a", "b", "c"], ["1", "2", "3"]])
        row = get_row_as_list(df, 0)

        assert row == ["a", "b", "c"]

    def test_row_with_whitespace(self):
        """Test that whitespace is stripped."""
        df = pd.DataFrame([["  a  ", " b", "c  "], ["1", "2", "3"]])
        row = get_row_as_list(df, 0)

        assert row == ["a", "b", "c"]

    def test_invalid_row_index(self):
        """Test that invalid row indices return empty list."""
        df = pd.DataFrame([["a", "b"], ["1", "2"]])

        assert get_row_as_list(df, -1) == []
        assert get_row_as_list(df, 10) == []

    def test_numeric_values_converted_to_string(self):
        """Test that numeric values are converted to strings."""
        df = pd.DataFrame([[1, 2.5, "text"]])
        row = get_row_as_list(df, 0)

        assert row == ["1", "2.5", "text"]


class TestGetFileExtension:
    """Tests for get_file_extension function."""

    def test_basic_extensions(self):
        """Test basic file extensions."""
        assert get_file_extension("file.csv") == "csv"
        assert get_file_extension("file.xlsx") == "xlsx"
        assert get_file_extension("file.TXT") == "txt"  # Should be lowercase

    def test_multiple_dots(self):
        """Test filename with multiple dots."""
        assert get_file_extension("my.file.name.csv") == "csv"

    def test_path_with_directories(self):
        """Test filename with directory path."""
        assert get_file_extension("/path/to/file.xlsx") == "xlsx"
        assert get_file_extension("C:\\Users\\file.csv") == "csv"

    def test_no_extension(self):
        """Test filename without extension."""
        assert get_file_extension("filename") == ""
