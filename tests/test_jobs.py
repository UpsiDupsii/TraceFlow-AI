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
    
@patch("app.services.llm.llm_service.generate_response", new_callable=AsyncMock)
async def test_create_job_success(mock_generate, async_client: AsyncClient):
    """
    Test successful job submission by mocking the Ollama service layer.
    """
    # Mock LLM execution response
    mock_generate.return_value = {
        "response": "Asynchronous handling prevents blocking I/O operations.",
        "execution_time_ms": 120.5,
        "eval_count": 10
    }
    payload = {
        "provider": "ollama",
        "model_name": "qwen2.5:3b",
        "prompt": "Why use async?"
    }
    
    response = await async_client.post("/api/v1/jobs", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert data["output"] == "Asynchronous handling prevents blocking I/O operations."
    assert data["latency_ms"] == 120.5
    mock_generate.assert_called_once_with(prompt="Why use async?", model_name="qwen2.5:3b")