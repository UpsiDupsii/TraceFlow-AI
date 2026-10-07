import asyncio
import sys
from app.core.config import settings
from app.core.logging import setup_logging, logger
from app.services.kafka_producer import kafka_producer
from app.services.kafka_consumer import kafka_consumer
from app.services.redis_service import redis_service

setup_logging()

async def main():
    logger.info(f"Starting TraceFlow-AI Worker [{settings.ENVIRONMENT}]...")
    
    await redis_service.connect()
    await kafka_producer.start()
    await kafka_consumer.start()

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Initiating graceful shutdown of TraceFlow-AI Worker...")
        await kafka_consumer.stop()
        await kafka_producer.stop()
        await redis_service.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
        sys.exit(0)