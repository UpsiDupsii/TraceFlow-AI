import time
from ollama import AsyncClient
from app.core.config import settings
from app.core.logging import logger

class LLMService:
    def __init__(self):
        self.client = AsyncClient(host=settings.OLLAMA_BASE_URL)
        
    async def generate_response(self, prompt: str, model_name: str) -> dict:
        start_time = time.perf_counter()
        
        logger.info(f"Executing LLM generation | model={model_name}")
        
        # Async invocation to local Ollama server
        response = await self.client.generate(
            model=model_name,
            prompt=prompt
        )
        
        execution_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
        
        logger.info(f"LLM generation complete | model={model_name} | latency={execution_time_ms}ms")

        return {
            "response": response.get("response", ""),
            "execution_time_ms": execution_time_ms,
            "eval_count": response.get("eval_count", 0)  # Total tokens output
        }
        
llm_service = LLMService()