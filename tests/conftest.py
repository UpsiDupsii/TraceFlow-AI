import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.fixture
async def async_client():
    """
    Provides an asynchronous HTTP client configured for FastAPI testing.
    Uses ASGITransport to hit the app directly in memory.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client