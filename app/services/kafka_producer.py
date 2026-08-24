import json
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logging import logger

class KafkaProducerService:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        
    async def start(self):
        """Initializes and starts the async Kafka producer connection."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await self.producer.start()
        logger.info("Kafka Producer successfully connected to bootstrap servers.")
    
    async def stop(self):
        """Gracefully shuts down the Kafka producer connection."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer connection closed gracefully.")

    async def send_event(self, topic: str, event_data: dict):
        """Publishes an event payload asynchronously to a specified Kafka topic."""
        if not self.producer:
            logger.error("Attempted to send event while Kafka Producer was uninitialized.")
            raise RuntimeError("Kafka Producer is not initialized.")
        
        await self.producer.send_and_wait(topic, value=event_data)
        logger.info(f"Event successfully published to Kafka topic '{topic}'")

kafka_producer = KafkaProducerService()