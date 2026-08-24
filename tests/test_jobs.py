from unittest.mock import patch, AsyncMock
from httpx import AsyncClient

async def test_create_job_validation_error(async_client: AsyncClient):
    """
    Test that sending an empty prompt triggers a 422 validation error from Pydantic.
    """
    payload = {
        "provider": "ollama",
        "model_name": "qwen2.5:3b",
        "prompt": ""  # Violates min_length=1
    }
    
    response = await async_client.post("/api/v1/jobs", json=payload)
    
    assert response.status_code == 422
    assert "detail" in response.json()
    
@patch("app.services.redis_service.redis_service.set_job", new_callable=AsyncMock)
@patch("app.services.kafka_producer.kafka_producer.send_event", new_callable=AsyncMock)
async def test_create_job_async_success(mock_send_event, mock_set_job, async_client: AsyncClient):
    """Test 202 Accepted response on valid job submission."""

    payload = {
        "provider": "ollama",
        "model_name": "qwen2.5:3b",
        "prompt": "What is event-driven architecture?"
    }
    
    response = await async_client.post("/api/v1/jobs", json=payload)
    
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "PENDING"
    assert "job_id" in data
    mock_set_job.assert_called_once()
    mock_send_event.assert_called_once()

@patch("app.services.redis_service.redis_service.get_job", new_callable=AsyncMock)
async def test_get_job_polling_success(mock_get_job, async_client: AsyncClient):
    """Test 200 OK retrieval of completed job status from Redis."""
    mock_get_job.return_value = {
        "job_id": "job_12345",
        "status": "COMPLETED",
        "provider": "ollama",
        "model_name": "qwen2.5:3b",
        "output": "Decoupled system response.",
        "latency_ms": 145.2,
        "message": "Job executed successfully."
    }
    
    response = await async_client.get("/api/v1/jobs/job_12345")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert data["output"] == "Decoupled system response."

@patch("app.services.redis_service.redis_service.get_job", new_callable=AsyncMock)
async def test_get_job_not_found(mock_get_job, async_client: AsyncClient):
    """Test 404 Not Found for missing or expired job ID."""
    mock_get_job.return_value = None
    
    response = await async_client.get("/api/v1/jobs/non_existent_id")
    
    assert response.status_code == 404