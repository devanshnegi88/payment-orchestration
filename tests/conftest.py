"""
pytest configuration and shared fixtures.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_db():
    """Mock async database session."""
    db = AsyncMock()
    db.execute = AsyncMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.close = AsyncMock()
    db.add = MagicMock()
    return db


@pytest.fixture
def mock_redis():
    """In-memory Redis mock for circuit breaker tests."""
    store = {}

    class MockRedis:
        async def get(self, key):
            val = store.get(key)
            return val.encode() if isinstance(val, str) else val

        async def set(self, key, value, *args, **kwargs):
            store[key] = str(value)

        async def incr(self, key):
            current = int(store.get(key, "0"))
            store[key] = str(current + 1)
            return current + 1

        async def delete(self, *keys):
            for k in keys:
                store.pop(k, None)

        async def expire(self, key, seconds):
            pass

    return MockRedis()
