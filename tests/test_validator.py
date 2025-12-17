"""
Tests for validator service.
"""

import pytest

from app.services.validator import validate_file_extension


class TestValidateFileExtension:
    """Tests for validate_file_extension function."""

    def test_valid_extensions(self):
        """Test that valid extensions pass."""
        allowed = ["csv", "xlsx", "xls", "txt"]

        assert validate_file_extension("file.csv", allowed) is None
        assert validate_file_extension("file.xlsx", allowed) is None
        assert validate_file_extension("file.xls", allowed) is None
        assert validate_file_extension("file.txt", allowed) is None

    def test_case_insensitive(self):
        """Test that extension check is case-insensitive."""
        allowed = ["csv", "xlsx"]

        assert validate_file_extension("FILE.CSV", allowed) is None
        assert validate_file_extension("file.XLSX", allowed) is None

    def test_invalid_extension(self):
        """Test that invalid extensions return error message."""
        allowed = ["csv", "xlsx"]

        error = validate_file_extension("file.pdf", allowed)
        assert error is not None
        assert ".pdf" in error
        assert "not supported" in error

    def test_no_extension(self):
        """Test that files without extension return error."""
        allowed = ["csv", "xlsx"]

        error = validate_file_extension("filename", allowed)
        assert error is not None
        assert "no extension" in error.lower()

    def test_no_filename(self):
        """Test that empty filename returns error."""
        allowed = ["csv", "xlsx"]

        error = validate_file_extension("", allowed)
        assert error is not None
        assert "No filename" in error

    def test_path_with_directories(self):
        """Test that paths with directories work."""
        allowed = ["csv"]

        assert validate_file_extension("/path/to/file.csv", allowed) is None
        assert validate_file_extension("C:\\Users\\file.csv", allowed) is None


# Note: async tests for validate_file and check_duplicate_file
# would require database fixtures and are tested as integration tests
