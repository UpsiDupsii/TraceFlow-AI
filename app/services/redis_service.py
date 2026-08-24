import json
import redis.asyncio as aioredis
from app.core.config import settings
from app.core.logging import logger

class RedisService:
    def __init__(self):
        self.client: aioredis.Redis | None = None
        
    async def connect(self):
        self.client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True
        )
        logger.info("Redis client connected successfully.")
    
    async def disconnect(self):
        if self.client:
            await self.client.aclose()
            logger.info("Redis client connection closed.")
    
    async def set_job(self, job_id: str, job_data: dict, ttl_seconds: int = 86400):
        if not self.client:
            raise RuntimeError("Redis client is not initialized.")
        key = f"job:{job_id}"
        await self.client.set(key, json.dumps(job_data), ex=ttl_seconds)
        
    async def get_job(self, job_id: str) -> dict | None:
        if not self.client:
            raise RuntimeError("Redis client is not initialized.")
        key = f"job:{job_id}"
        data = await self.client.get(key)
        return json.loads(data) if data else None

redis_service = RedisService()
