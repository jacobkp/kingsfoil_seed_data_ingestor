"""
Schema documentation router - displays column definitions and metadata for each data source.
"""

import asyncpg
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db

router = APIRouter(prefix="/schema", tags=["schema"])
templates = Jinja2Templates(directory="app/templates")

# Category definitions with display names
CATEGORIES = {
    "pfs": {
        "name": "Physician Fee Schedule",
        "description": "Medicare payment amounts for physician services, including RVUs, GPCIs, and conversion factors.",
        "sources": ["PFS_RVU", "PFS_GPCI", "PFS_LOCALITY", "PFS_ANES_CF", "PFS_OPPS_CAP"],
    },
    "ncci": {
        "name": "NCCI Edits",
        "description": "National Correct Coding Initiative edits for procedure bundling (PTP) and quantity limits (MUE).",
        "sources": ["NCCI_PTP", "NCCI_MUE_DME", "NCCI_MUE_PRAC", "NCCI_MUE_OPH"],
    },
    "hcpcs": {
        "name": "HCPCS Codes",
        "description": "Healthcare Common Procedure Coding System Level II codes and descriptions.",
        "sources": ["HCPCS"],
    },
}


@router.get("/{category}", response_class=HTMLResponse)
async def schema_documentation(
    request: Request,
    category: str,
    source: str = None,
    conn: asyncpg.Connection = Depends(get_db),
):
    """Display schema documentation for a category of data sources."""
    category = category.lower()

    if category not in CATEGORIES:
        raise HTTPException(status_code=404, detail=f"Category not found: {category}")

    cat_info = CATEGORIES[category]

    # Get all sources in this category
    sources = await conn.fetch(
        """
        SELECT id, source_code, source_name, description, target_table, update_frequency
        FROM meta.data_sources
        WHERE source_code = ANY($1) AND is_active = TRUE
        ORDER BY display_order, source_name
        """,
        cat_info["sources"],
    )

    if not sources:
        raise HTTPException(status_code=404, detail=f"No sources found for category: {category}")

    sources = [dict(s) for s in sources]

    # Determine which source to show (default to first)
    if source:
        selected_source = next((s for s in sources if s["source_code"] == source.upper()), sources[0])
    else:
        selected_source = sources[0]

    # Get columns for the selected source
    columns = await conn.fetch(
        """
        SELECT
            cc.id,
            cc.internal_name,
            cc.display_name,
            cc.data_type,
            cc.is_nullable,
            cc.is_required,
            cc.semantic_context,
            cc.analyzer_usage,
            cc.display_order,
            cm.source_headers
        FROM meta.canonical_columns cc
        LEFT JOIN meta.column_mappings cm ON cm.canonical_column_id = cc.id
        WHERE cc.source_id = $1
        ORDER BY cc.display_order, cc.internal_name
        """,
        selected_source["id"],
    )

    columns = [dict(c) for c in columns]

    # Get latest version info for this source
    latest_version = await conn.fetchrow(
        """
        SELECT version_label, variant, record_count, imported_at
        FROM meta.data_versions
        WHERE source_id = $1 AND status = 'completed'
        ORDER BY imported_at DESC
        LIMIT 1
        """,
        selected_source["id"],
    )

    return templates.TemplateResponse(
        "schema.html",
        {
            "request": request,
            "category": category,
            "category_name": cat_info["name"],
            "category_description": cat_info["description"],
            "sources": sources,
            "selected_source": selected_source,
            "columns": columns,
            "latest_version": dict(latest_version) if latest_version else None,
            "active_page": "schema",
            "categories": CATEGORIES,
        },
    )


@router.get("/", response_class=HTMLResponse)
async def schema_index(request: Request):
    """Redirect to PFS category by default."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/schema/pfs", status_code=302)
