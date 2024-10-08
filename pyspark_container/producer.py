from kafka import KafkaProducer
import time
import json

class Producer:
    def __init__(self):
        time.sleep(5)
        self.producer = KafkaProducer(
                        bootstrap_servers=['kafka:9092'],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
