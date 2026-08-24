from unittest.mock import AsyncMock, patch
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.services.redis_service import redis_service

@pytest.fixture
async def async_client():
    """
    Provides an asynchronous HTTP client configured for FastAPI testing.
    Uses ASGITransport to hit the app directly in memory.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    
@pytest.fixture(autouse=True)
def mock_infrastructure():
    
    redis_service.client = AsyncMock()
    
    with patch("app.services.kafka_producer.kafka_producer.start", new_callable=AsyncMock), \
         patch("app.services.kafka_producer.kafka_producer.stop", new_callable=AsyncMock), \
         patch("app.services.kafka_producer.kafka_producer.send_event", new_callable=AsyncMock), \
         patch("app.services.kafka_consumer.kafka_consumer.start", new_callable=AsyncMock), \
         patch("app.services.kafka_consumer.kafka_consumer.stop", new_callable=AsyncMock), \
         patch("app.services.redis_service.redis_service.connect", new_callable=AsyncMock), \
         patch("app.services.redis_service.redis_service.disconnect", new_callable=AsyncMock):
        yield
        
    redis_service.client = None
    
    