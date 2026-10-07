import asyncio
import json
import time
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.logging import logger
from app.services.llm import llm_service
from app.services.redis_service import redis_service
from app.services.kafka_producer import kafka_producer

class KafkaConsumerService:
    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running: bool = False
        self.consumer_task: asyncio.Task | None = None

    async def start(self):
        """Initializes and starts listening to the Kafka jobs topic."""
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_JOBS,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="traceflow_llm_workers",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        
        await self.consumer.start()
        self.is_running = True
        logger.info(f"Kafka Consumer connected to topic '{settings.KAFKA_TOPIC_JOBS}'.")
        
        self.consumer_task = asyncio.create_task(self._consume_loop())

    async def _route_to_dlq(self, event_data: dict, error_message: str, retry_count: int):
        """Dispatches an unprocessable or repeatedly failing job to the Dead Letter Queue."""
        dlq_payload = {
            **event_data,
            "error": error_message,
            "retries_attempted": retry_count,
            "failed_at": time.time()
        }
        await kafka_producer.send_event(topic=settings.KAFKA_TOPIC_DLQ, event_data=dlq_payload)
        logger.warning(f"Job routed to DLQ | job_id={event_data.get('job_id')} | topic={settings.KAFKA_TOPIC_DLQ}")

    async def _execute_with_retry(self, prompt: str, model_name: str) -> dict:
        """Executes LLM request with exponential backoff on transient errors."""
        last_exception = None
        for attempt in range(1, settings.MAX_RETRIES + 1):
            try:
                return await llm_service.generate_response(prompt=prompt, model_name=model_name)
            except Exception as exc:
                last_exception = exc
                if attempt == settings.MAX_RETRIES:
                    break
                delay = settings.RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
                logger.warning(
                    f"LLM call failed (attempt {attempt}/{settings.MAX_RETRIES}) | "
                    f"Retrying in {delay}s | error={str(exc)}"
                )
                await asyncio.sleep(delay)
        
        raise last_exception

    async def _consume_loop(self):
        """Main loop consuming jobs, processing execution, and handling failures."""
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
                    result = await self._execute_with_retry(prompt=prompt, model_name=model_name)

                    completed_data = {
                        "job_id": job_id,
                        "status": "COMPLETED",
                        "provider": provider,
                        "model_name": model_name,
                        "output": result["response"],
                        "latency_ms": result["execution_time_ms"],
                        "message": "Job executed successfully."
                    }
                    await redis_service.set_job(job_id=job_id, job_data=completed_data)
                    logger.info(f"Job COMPLETED | job_id={job_id}")

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Job FAILED permanently after {settings.MAX_RETRIES} attempts | job_id={job_id} | error={error_msg}")

                    failed_data = {
                        "job_id": job_id,
                        "status": "FAILED",
                        "provider": provider,
                        "model_name": model_name,
                        "output": None,
                        "latency_ms": None,
                        "message": f"Execution error: {error_msg}"
                    }
                    await redis_service.set_job(job_id=job_id, job_data=failed_data)
                    await self._route_to_dlq(event_data, error_msg, settings.MAX_RETRIES)

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
