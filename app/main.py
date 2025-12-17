"""
FastAPI application entry point.
"""

from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from pathlib import Path

from app.config import get_settings
from app.database import db, get_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    settings = get_settings()

    # Startup
    print(f"Starting {settings.app_name}...")
    await db.connect()
    print("Database connected.")

    # Ensure temp upload directory exists
    settings.temp_upload_dir.mkdir(parents=True, exist_ok=True)
    print(f"Temp upload directory: {settings.temp_upload_dir}")

    yield

    # Shutdown
    print("Shutting down...")
    await db.disconnect()
    print("Database disconnected.")


def create_app() -> FastAPI:
    """Application factory."""
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        debug=settings.debug,
        lifespan=lifespan,
    )

    # Static files
    static_path = Path(__file__).parent.parent / "static"
    if static_path.exists():
        app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    # Include routers
    from app.routers import dashboard, upload, schema
    app.include_router(dashboard.router)
    app.include_router(upload.router)
    app.include_router(schema.router)

    return app


app = create_app()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}
