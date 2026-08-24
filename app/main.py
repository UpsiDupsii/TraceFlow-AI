from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.config import settings
from app.core.logging import setup_logging, logger
from app.services.kafka_producer import kafka_producer
from app.services.kafka_consumer import kafka_consumer
from app.services.redis_service import redis_service
from app.api.v1.jobs import router as jobs_router

setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.PROJECT_NAME} [{settings.ENVIRONMENT}]...")
    await kafka_producer.start()
    await kafka_consumer.start()
    await redis_service.connect()
    yield
    logger.info(f"Shutting down {settings.PROJECT_NAME}...")
    await kafka_producer.stop()
    await kafka_consumer.stop()
    await redis_service.disconnect()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
)

# Include v1 Router
app.include_router(jobs_router, prefix=settings.API_V1_STR)

@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "project": settings.PROJECT_NAME,
        "version": settings.VERSION
    }