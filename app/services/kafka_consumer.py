import asyncio
import json
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.logging import logger
from app.services.llm import llm_service
from app.services.redis_service import redis_service

class KafkaConsumerService:
    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running: bool = False
        self.consumer_task: asyncio.Task | None = None
    
    async def start(self):
        """Initializes and starts listening to the Kafka topic in the background."""
        self.consumer = AIOKafkaConsumer(
            "llm_jobs",
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="traceflow_llm_workers",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        
        await self.consumer.start()
        self.is_running = True
        logger.info("Kafka Consumer successfully connected to topic 'llm_jobs'.")
        
        # Spawn asynchronous consumer loop task
        self.consumer_task = asyncio.create_task(self._consume_loop())
        
    async def _consume_loop(self):
        """Infinite loop polling for incoming events from the Kafka topic."""
        try:
            async for msg in self.consumer:
                if not self.is_running:
                    break
                event_data = msg.value
                job_id = event_data.get("job_id")
                prompt = event_data.get("prompt")
                model_name = event_data.get("model_name")
                provider = event_data.get("provider")
                
                logger.info(f"Consumer picked up job | job_id={job_id} | model={model_name}")

                try:
                    result = await llm_service.generate_response(
                        prompt=prompt,
                        model_name=model_name
                    )

                    completed_data = {
                        "job_id": job_id,
                        "status": "COMPLETED",
                        "provider": provider,
                        "model_name": model_name,
                        "output": result["response"],
                        "latency_ms": result["execution_time_ms"],
                        "message": "Job executed successfully."
                    }

                    # Update status to COMPLETED in Redis
                    await redis_service.set_job(job_id=job_id, job_data=completed_data)
                    logger.info(f"Job state updated to COMPLETED in Redis | job_id={job_id}")

                except Exception as e:
                    logger.error(f"Error processing job | job_id={job_id} | error={str(e)}")
                    failed_data = {
                        "job_id": job_id,
                        "status": "FAILED",
                        "provider": provider,
                        "model_name": model_name,
                        "output": None,
                        "latency_ms": None,
                        "message": f"Execution error: {str(e)}"
                    }
                    await redis_service.set_job(job_id=job_id, job_data=failed_data)

        except asyncio.CancelledError:
            logger.info("Kafka Consumer loop cancelled.")
        except Exception as e:
            logger.error(f"Unexpected error in Kafka Consumer loop: {str(e)}")
        
    async def stop(self):
        self.is_running = False
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka Consumer connection closed gracefully.")

kafka_consumer = KafkaConsumerService()
