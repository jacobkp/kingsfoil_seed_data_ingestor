"""
Dashboard router - main dashboard showing all data sources.
"""

import asyncpg
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db

router = APIRouter(tags=["dashboard"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db),
):
    """Display main dashboard with all data sources."""
    # Get all data sources with their latest version info
    sources = await conn.fetch(
        """
        SELECT
            ds.id,
            ds.source_code,
            ds.source_name,
            ds.category,
            ds.description,
            ds.target_table,
            ds.update_frequency,
            ds.display_order,
            dv.version_label AS latest_version,
            dv.variant AS latest_variant,
            dv.record_count AS latest_record_count,
            dv.imported_at AS latest_imported_at,
            dv.is_current
        FROM meta.data_sources ds
        LEFT JOIN LATERAL (
            SELECT version_label, variant, record_count, imported_at, is_current
            FROM meta.data_versions
            WHERE source_id = ds.id AND status = 'completed'
            ORDER BY imported_at DESC
            LIMIT 1
        ) dv ON TRUE
        WHERE ds.is_active = TRUE
        ORDER BY ds.category, ds.display_order, ds.source_name
        """
    )

    # Group by category
    categories = {}
    for source in sources:
        category = source["category"]
        if category not in categories:
            categories[category] = []
        categories[category].append(dict(source))

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "categories": categories,
            "active_page": "dashboard",
        },
    )
