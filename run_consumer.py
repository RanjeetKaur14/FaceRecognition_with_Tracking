from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import os

from services.kafka_consumer_service import (
    KafkaConsumerService
)

consumer_service = KafkaConsumerService()

consumer = KafkaConsumer(
    "face-events",
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:29092"
    ),
    auto_offset_reset="latest",
    group_id="face-recognition-group"
)

executor = ThreadPoolExecutor(
    max_workers=4
)

print("Kafka Consumer Started...")

for message in consumer:

    event = message.value.decode("utf-8")

    executor.submit(
        consumer_service.consume,
        event
    )