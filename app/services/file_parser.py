"""
File parsing service.
Handles CSV, XLSX, XLS, and TXT files uniformly.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple


def parse_file(file_path: str) -> Tuple[pd.DataFrame, str]:
    """
    Parse a file into a pandas DataFrame.

    All files are read with:
    - No header assumption (header=None)
    - All values as strings (dtype=str)
    - No automatic NA filtering (na_filter=False)

    Supports: .csv, .xlsx, .xls, .txt

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (DataFrame, detected_extension)

    Raises:
        ValueError: If file type is unsupported
        FileNotFoundError: If file doesn't exist
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    ext = path.suffix.lower()

    if ext == ".csv":
        # Read CSV with various encoding fallbacks
        df = _read_csv(file_path)

    elif ext == ".xlsx":
        # Read modern Excel format
        df = pd.read_excel(
            file_path,
            header=None,
            dtype=str,
            na_filter=False,
            engine="openpyxl",
        )

    elif ext == ".xls":
        # Read legacy Excel format
        df = pd.read_excel(
            file_path,
            header=None,
            dtype=str,
            na_filter=False,
            engine="xlrd",
        )

    elif ext == ".txt":
        # Try to detect delimiter for text files
        df = _read_txt(file_path)

    else:
        raise ValueError(f"Unsupported file type: {ext}")

    return df, ext


def _read_csv(file_path: str) -> pd.DataFrame:
    """
    Read CSV file with encoding detection.

    Tries UTF-8 first, then falls back to latin-1.
    """
    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            return pd.read_csv(
                file_path,
                header=None,
                dtype=str,
                na_filter=False,
                encoding=encoding,
            )
        except UnicodeDecodeError:
            continue

    # Last resort: read with errors='replace'
    return pd.read_csv(
        file_path,
        header=None,
        dtype=str,
        na_filter=False,
        encoding="utf-8",
        encoding_errors="replace",
    )


def _read_txt(file_path: str) -> pd.DataFrame:
    """
    Read text file with delimiter detection.

    Tries tab-delimited first, then comma, then pipe.
    """
    # Read first few lines to detect delimiter
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)

    # Count potential delimiters
    tab_count = sample.count("\t")
    comma_count = sample.count(",")
    pipe_count = sample.count("|")

    # Choose delimiter with highest count
    if tab_count >= comma_count and tab_count >= pipe_count:
        sep = "\t"
    elif pipe_count >= comma_count:
        sep = "|"
    else:
        sep = ","

    return pd.read_csv(
        file_path,
        header=None,
        dtype=str,
        na_filter=False,
        sep=sep,
        encoding="utf-8",
        encoding_errors="replace",
    )


def get_row_as_list(df: pd.DataFrame, row_index: int) -> list[str]:
    """
    Get a row from DataFrame as a list of strings.

    Args:
        df: DataFrame
        row_index: Row index (0-based)

    Returns:
        List of cell values as strings, stripped of whitespace
    """
    if row_index < 0 or row_index >= len(df):
        return []

    return [str(val).strip() for val in df.iloc[row_index].tolist()]


def get_file_extension(filename: str) -> str:
    """
    Extract file extension from filename.

    Args:
        filename: Original filename

    Returns:
        Lowercase extension without dot (e.g., 'csv', 'xlsx')
    """
    return Path(filename).suffix.lower().lstrip(".")
