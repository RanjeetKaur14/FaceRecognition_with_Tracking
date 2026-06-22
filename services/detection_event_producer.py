from kafka import KafkaProducer
import json
import os


class DetectionEventProducer:

    def __init__(self):

        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:29092"
            ),
            value_serializer=lambda v:
                json.dumps(v).encode("utf-8")
        )

        print("Kafka Producer Connected")


    def publish_detection_event(self, event):

        self.producer.send(
            "detection-events",
            event
        )

        self.producer.flush()

        print("Detection Event Published")