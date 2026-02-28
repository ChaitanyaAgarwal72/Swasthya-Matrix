import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "health-stream"

df = pd.read_csv("data/synthetic_kafka_stream.csv")
print(f"Starting continuous stream to Kafka topic: '{topic_name}'...")

for index, row in df.iterrows():
    payload = row.to_dict()
    producer.send(topic_name, value=payload)
    print(f"Sent: {payload.get('State')} | Date: {payload.get('Date')}")
    time.sleep(0.5)