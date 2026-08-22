from httpx import AsyncClient
from app.core.config import settings

async def test_health_check(async_client: AsyncClient):
    """
    Test that the /health endpoint returns a 200 OK status
    and the correct project metadata.
    """
    response = await async_client.get("/health")

    # Assert HTTP status code
    assert response.status_code == 200
    
    # Assert JSON response structure
    data = response.json()
    assert data["status"] == "healthy"
    assert data["project"] == settings.PROJECT_NAME
    assert data["version"] == settings.VERSION