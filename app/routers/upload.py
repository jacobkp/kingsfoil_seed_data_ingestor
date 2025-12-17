"""
Upload router - handles file upload, validation, and ingestion.

Implements the stateless re-validation approach:
1. POST /upload/{source_code}/validate - Upload file, validate, return hidden form fields
2. POST /upload/{source_code}/ingest - Re-validate, ingest, cleanup temp file
"""

import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.database import get_db
from app.services.file_parser import parse_file, get_file_extension
from app.services.header_detector import detect_header_row
from app.services.validator import validate_file, check_duplicate_file, validate_file_extension
from app.services.ingestor import ingest_file

router = APIRouter(prefix="/upload", tags=["upload"])
templates = Jinja2Templates(directory="app/templates")
settings = get_settings()


# ============================================================
# Helper Functions
# ============================================================


def ensure_temp_dir():
    """Ensure temp upload directory exists."""
    settings.temp_upload_dir.mkdir(parents=True, exist_ok=True)


def get_temp_path(upload_id: str, file_ext: str) -> Path:
    """Get temp file path for an upload."""
    return settings.temp_upload_dir / f"{upload_id}.{file_ext}"


def compute_file_hash(content: bytes) -> str:
    """Compute SHA-256 hash of file content."""
    return hashlib.sha256(content).hexdigest()


async def get_source_info(conn: asyncpg.Connection, source_code: str) -> dict:
    """Get data source info from database."""
    source = await conn.fetchrow(
        """
        SELECT id, source_code, source_name, category, description,
               file_formats, target_table, update_frequency
        FROM meta.data_sources
        WHERE source_code = $1 AND is_active = TRUE
        """,
        source_code.upper(),
    )
    if not source:
        raise HTTPException(status_code=404, detail=f"Data source not found: {source_code}")
    return dict(source)


async def get_column_mappings(conn: asyncpg.Connection, source_id: int) -> dict:
    """Get column mappings for a source."""
    rows = await conn.fetch(
        """
        SELECT cc.internal_name, cc.is_required, cm.source_headers
        FROM meta.canonical_columns cc
        JOIN meta.column_mappings cm ON cm.canonical_column_id = cc.id
        WHERE cc.source_id = $1
        """,
        source_id,
    )

    mappings = {}
    for row in rows:
        mappings[row["internal_name"]] = {
            "headers": row["source_headers"],
            "is_required": row["is_required"],
        }
    return mappings


async def get_last_upload(conn: asyncpg.Connection, source_id: int, variant: Optional[str] = None) -> Optional[dict]:
    """Get info about the last successful upload for this source."""
    if variant:
        row = await conn.fetchrow(
            """
            SELECT version_label, variant, file_name, record_count, imported_at
            FROM meta.data_versions
            WHERE source_id = $1 AND variant = $2 AND status = 'completed'
            ORDER BY imported_at DESC
            LIMIT 1
            """,
            source_id, variant,
        )
    else:
        row = await conn.fetchrow(
            """
            SELECT version_label, variant, file_name, record_count, imported_at
            FROM meta.data_versions
            WHERE source_id = $1 AND status = 'completed'
            ORDER BY imported_at DESC
            LIMIT 1
            """,
            source_id,
        )

    if row:
        return dict(row)
    return None


def build_version_label(year: int, quarter: int) -> str:
    """Build version label from year and quarter."""
    return f"{year}-Q{quarter}"


def get_effective_date(year: int, quarter: int) -> datetime:
    """Get effective date from year and quarter."""
    quarter_start_months = {1: 1, 2: 4, 3: 7, 4: 10}
    month = quarter_start_months[quarter]
    return datetime(year, month, 1)


# ============================================================
# Routes
# ============================================================


@router.get("/{source_code}", response_class=HTMLResponse)
async def upload_page(
    request: Request,
    source_code: str,
    conn: asyncpg.Connection = Depends(get_db),
):
    """Display upload page for a data source."""
    source = await get_source_info(conn, source_code)
    last_upload = await get_last_upload(conn, source["id"])

    # Determine if this source has variants
    has_variants = source_code.upper() == "NCCI_PTP"
    variants = ["HOSPITAL", "PRACTITIONER"] if has_variants else []

    # NCCI PTP files come in multiple parts per quarter - support appending
    supports_multi_part = source_code.upper() == "NCCI_PTP"

    # Get current year and available years
    # Include next year for advance CMS data releases (e.g., 2026 data released in late 2025)
    current_year = datetime.now().year
    years = list(range(current_year + 1, current_year - 5, -1))  # Next year through 5 years back
    quarters = [1, 2, 3, 4]

    # Get existing versions for this source (for multi-part append)
    existing_versions = []
    if supports_multi_part:
        existing_versions = await conn.fetch(
            """
            SELECT v.id, v.version_label, v.variant, v.record_count,
                   COALESCE(v.part_count, 1) as part_count,
                   v.file_name,
                   v.imported_at
            FROM meta.data_versions v
            WHERE v.source_id = $1 AND v.status = 'completed'
            ORDER BY v.version_label DESC, v.variant
            LIMIT 20
            """,
            source["id"],
        )
        existing_versions = [dict(v) for v in existing_versions]

    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "source": source,
            "last_upload": last_upload,
            "has_variants": has_variants,
            "variants": variants,
            "supports_multi_part": supports_multi_part,
            "existing_versions": existing_versions,
            "years": years,
            "quarters": quarters,
            "current_year": current_year,
            "current_quarter": (datetime.now().month - 1) // 3 + 1,
            "active_page": "upload",
        },
    )


@router.post("/{source_code}/validate", response_class=HTMLResponse)
async def validate_upload(
    request: Request,
    source_code: str,
    file: UploadFile = File(...),
    year: int = Form(...),
    quarter: int = Form(...),
    variant: Optional[str] = Form(None),
    conn: asyncpg.Connection = Depends(get_db),
):
    """
    Validate an uploaded file.
    Saves to temp directory and returns validation results with hidden form fields.
    """
    source = await get_source_info(conn, source_code)

    # Validate file extension
    ext_error = validate_file_extension(file.filename, settings.allowed_extensions)
    if ext_error:
        return templates.TemplateResponse(
            "upload_validate.html",
            {
                "request": request,
                "source": source,
                "validation_passed": False,
                "errors": [ext_error],
                "warnings": [],
            },
        )

    # Read file content
    content = await file.read()
    file_size = len(content)

    # Check file size
    max_size = settings.max_upload_size_mb * 1024 * 1024
    if file_size > max_size:
        return templates.TemplateResponse(
            "upload_validate.html",
            {
                "request": request,
                "source": source,
                "validation_passed": False,
                "errors": [f"File size ({file_size / 1024 / 1024:.1f} MB) exceeds maximum ({settings.max_upload_size_mb} MB)"],
                "warnings": [],
            },
        )

    # Compute hash
    file_hash = compute_file_hash(content)

    # Check if this is a multi-part source
    is_multi_part_source = source["source_code"].upper() == "NCCI_PTP"

    # Check for duplicate file
    # For multi-part sources, also check the data_version_parts table
    duplicate = await check_duplicate_file(conn, source["id"], file_hash)

    if duplicate and not is_multi_part_source:
        # For non-multi-part sources, block duplicate uploads
        return templates.TemplateResponse(
            "upload_validate.html",
            {
                "request": request,
                "source": source,
                "validation_passed": False,
                "errors": [
                    f"This exact file was already uploaded on {duplicate['imported_at'].strftime('%Y-%m-%d')} "
                    f"as version {duplicate['version_label']}"
                    + (f" ({duplicate['variant']})" if duplicate['variant'] else "")
                ],
                "warnings": [],
            },
        )
    elif is_multi_part_source:
        # For multi-part sources, check if this exact file was already added as a part
        existing_part = await conn.fetchrow(
            """
            SELECT vp.part_number, v.version_label, v.variant
            FROM meta.data_version_parts vp
            JOIN meta.data_versions v ON v.id = vp.data_version_id
            WHERE v.source_id = $1 AND vp.file_hash = $2
            """,
            source["id"], file_hash,
        )
        if existing_part:
            return templates.TemplateResponse(
                "upload_validate.html",
                {
                    "request": request,
                    "source": source,
                    "validation_passed": False,
                    "errors": [
                        f"This exact file was already uploaded as Part {existing_part['part_number']} "
                        f"of version {existing_part['version_label']} ({existing_part['variant']})"
                    ],
                    "warnings": [],
                },
            )

    # Save to temp file
    ensure_temp_dir()
    upload_id = str(uuid.uuid4())
    file_ext = get_file_extension(file.filename)
    temp_path = get_temp_path(upload_id, file_ext)
    temp_path.write_bytes(content)

    try:
        # Parse file
        df, _ = parse_file(str(temp_path))

        # Get column mappings
        column_mappings = await get_column_mappings(conn, source["id"])

        # Detect header row
        header_result = detect_header_row(df, column_mappings, max_scan_rows=settings.max_header_scan_rows)

        if not header_result["found"]:
            return templates.TemplateResponse(
                "upload_validate.html",
                {
                    "request": request,
                    "source": source,
                    "validation_passed": False,
                    "errors": [header_result["error"] or "Could not detect header row"],
                    "warnings": [],
                },
            )

        # Validate file contents
        validation_result = await validate_file(
            conn,
            source["id"],
            df,
            header_result["header_row_index"],
            header_result["column_map"],
        )

        # Build version label
        version_label = build_version_label(year, quarter)

        # Check if this is a multi-part source (NCCI PTP)
        supports_multi_part = source["source_code"].upper() == "NCCI_PTP"

        # Check if this version already exists
        existing_version = await conn.fetchrow(
            """
            SELECT id, status, record_count, COALESCE(part_count, 1) as part_count
            FROM meta.data_versions
            WHERE source_id = $1 AND version_label = $2 AND variant IS NOT DISTINCT FROM $3
            """,
            source["id"], version_label, variant,
        )

        version_exists = existing_version is not None
        version_status = existing_version["status"] if existing_version else None

        # For multi-part sources, check if we'll be appending
        will_append = False
        existing_record_count = 0
        existing_part_count = 0
        if supports_multi_part and existing_version and existing_version["status"] == "completed":
            will_append = True
            existing_record_count = existing_version["record_count"] or 0
            existing_part_count = existing_version["part_count"] or 1

        # Get data row count
        data_row_count = len(df) - header_result["header_row_index"] - 1

        return templates.TemplateResponse(
            "upload_validate.html",
            {
                "request": request,
                "source": source,
                "validation_passed": validation_result["passed"],
                "errors": validation_result["errors"],
                "warnings": validation_result["warnings"],
                "file_info": {
                    "name": file.filename,
                    "size": file_size,
                    "hash": file_hash,
                    "row_count": data_row_count,
                    "header_row": header_result["header_row_index"] + 1,  # 1-indexed for display
                },
                "column_map": header_result["column_map"],
                "unmapped_columns": header_result["unmapped_columns"],
                "version_label": version_label,
                "version_exists": version_exists,
                "version_status": version_status,
                # Multi-part append info
                "supports_multi_part": supports_multi_part,
                "will_append": will_append,
                "existing_record_count": existing_record_count,
                "existing_part_count": existing_part_count,
                # Hidden form fields for ingestion
                "upload_id": upload_id,
                "file_ext": file_ext,
                "file_hash": file_hash,
                "original_filename": file.filename,
                "file_size": file_size,
                "year": year,
                "quarter": quarter,
                "variant": variant,
                "header_row_index": header_result["header_row_index"],
            },
        )

    except Exception as e:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        return templates.TemplateResponse(
            "upload_validate.html",
            {
                "request": request,
                "source": source,
                "validation_passed": False,
                "errors": [f"Error processing file: {str(e)}"],
                "warnings": [],
            },
        )


@router.post("/{source_code}/ingest", response_class=HTMLResponse)
async def ingest_upload(
    request: Request,
    source_code: str,
    upload_id: str = Form(...),
    file_ext: str = Form(...),
    file_hash: str = Form(...),
    original_filename: str = Form(...),
    file_size: int = Form(...),
    year: int = Form(...),
    quarter: int = Form(...),
    variant: Optional[str] = Form(None),
    header_row_index: int = Form(...),
    mark_as_current: bool = Form(False),
    conn: asyncpg.Connection = Depends(get_db),
):
    """
    Ingest a validated file.
    Re-validates, ingests, and cleans up temp file.
    """
    source = await get_source_info(conn, source_code)

    # Reconstruct temp file path
    temp_path = get_temp_path(upload_id, file_ext)

    # Check temp file exists (session expired if not)
    if not temp_path.exists():
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "source": source,
                "error_title": "Session Expired",
                "error_message": "The upload session has expired. Please upload the file again.",
            },
        )

    # Read and verify hash
    content = temp_path.read_bytes()
    actual_hash = compute_file_hash(content)

    if actual_hash != file_hash:
        temp_path.unlink()
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "source": source,
                "error_title": "File Integrity Error",
                "error_message": "The uploaded file has been modified. Please upload again.",
            },
        )

    try:
        # Re-parse file (fast re-validation)
        df, _ = parse_file(str(temp_path))

        # Get column mappings
        column_mappings = await get_column_mappings(conn, source["id"])

        # Re-detect header (should be consistent)
        header_result = detect_header_row(df, column_mappings, max_scan_rows=settings.max_header_scan_rows)

        if not header_result["found"]:
            return templates.TemplateResponse(
                "upload_error.html",
                {
                    "request": request,
                    "source": source,
                    "error_title": "Validation Failed",
                    "error_message": header_result["error"] or "Could not detect header row",
                },
            )

        # Build version info
        version_label = build_version_label(year, quarter)
        effective_date = get_effective_date(year, quarter)

        # NCCI PTP supports multi-part uploads - append to existing version if one exists
        append_to_existing = source["source_code"].upper() == "NCCI_PTP"

        # Perform ingestion
        result = await ingest_file(
            conn,
            source_id=source["id"],
            source_code=source["source_code"],
            df=df,
            header_row_index=header_result["header_row_index"],
            column_map=header_result["column_map"],
            version_label=version_label,
            variant=variant,
            effective_date=effective_date,
            file_name=original_filename,
            file_hash=file_hash,
            file_size_bytes=file_size,
            mark_as_current=mark_as_current,
            append_to_existing=append_to_existing,
        )

        # Clean up temp file
        temp_path.unlink()

        if result["success"]:
            return templates.TemplateResponse(
                "upload_success.html",
                {
                    "request": request,
                    "source": source,
                    "version_label": version_label,
                    "variant": variant,
                    "result": result,
                    "marked_as_current": mark_as_current,
                    "file_info": {
                        "name": original_filename,
                        "size": file_size,
                    },
                },
            )
        else:
            return templates.TemplateResponse(
                "upload_error.html",
                {
                    "request": request,
                    "source": source,
                    "error_title": "Ingestion Failed",
                    "error_message": "; ".join(result["errors"][:3]),
                    "all_errors": result["errors"],
                },
            )

    except Exception as e:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "source": source,
                "error_title": "Ingestion Error",
                "error_message": str(e),
            },
        )


# ============================================================
# Temp File Cleanup
# ============================================================


def cleanup_old_temp_files(max_age_hours: int = 24):
    """
    Remove temp files older than max_age_hours.
    Can be called on startup or periodically.
    """
    from datetime import timedelta

    if not settings.temp_upload_dir.exists():
        return 0

    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    removed = 0

    for ext in settings.allowed_extensions:
        for f in settings.temp_upload_dir.glob(f"*.{ext}"):
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink()
                removed += 1

    return removed
