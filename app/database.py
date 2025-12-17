"""
Database connection management.
Uses asyncpg for async PostgreSQL access.
"""

import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from app.config import get_settings


class Database:
    """Database connection pool manager."""

    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        """Initialize connection pool."""
        settings = get_settings()
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
        )

    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        async with self.pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get a connection with transaction."""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn


# Global database instance
db = Database()


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """Dependency for FastAPI routes."""
    async with db.connection() as conn:
        yield conn
