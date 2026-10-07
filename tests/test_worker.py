import pytest
from unittest.mock import patch, AsyncMock
from app.services.kafka_consumer import KafkaConsumerService

@pytest.mark.asyncio
async def test_execute_with_retry_succeeds_first_try():
    service = KafkaConsumerService()
    with patch("app.services.llm.llm_service.generate_response", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = {"response": "Success", "execution_time_ms": 120.0}
        result = await service._execute_with_retry(prompt="test", model_name="qwen2.5:3b")
        
        assert result["response"] == "Success"
        assert mock_llm.call_count == 1

@pytest.mark.asyncio
async def test_execute_with_retry_exhausts_retries():
    service = KafkaConsumerService()
    with patch("app.services.llm.llm_service.generate_response", side_effect=RuntimeError("Ollama timeout")), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        
        with pytest.raises(RuntimeError) as exc_info:
            await service._execute_with_retry(prompt="test", model_name="qwen2.5:3b")
            
        assert "Ollama timeout" in str(exc_info.value)

@pytest.mark.asyncio
async def test_route_to_dlq_called_on_failure():
    service = KafkaConsumerService()
    event_payload = {
        "job_id": "job_9999",
        "provider": "ollama",
        "model_name": "qwen2.5:3b",
        "prompt": "Test prompt"
    }

    with patch("app.services.kafka_producer.kafka_producer.send_event", new_callable=AsyncMock) as mock_send_dlq:
        await service._route_to_dlq(event_payload, error_message="Inference failed", retry_count=3)
        
        mock_send_dlq.assert_called_once()
        call_args = mock_send_dlq.call_args[1]
        assert call_args["topic"] == "llm_jobs_dlq"
        assert call_args["event_data"]["job_id"] == "job_9999"
        assert call_args["event_data"]["error"] == "Inference failed"
        assert call_args["event_data"]["retries_attempted"] == 3
        