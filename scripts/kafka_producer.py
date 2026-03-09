import pandas as pd
import json
import time
from kafka import KafkaProducer
import os

source_file = "data/synthetic_kafka_stream.csv"

if not os.path.exists(source_file):
    print(f"ERROR: Cannot find {source_file}")
    exit()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "health-stream"
df = pd.read_csv(source_file)
unique_dates = df['Date'].unique()

print(f"Starting real data stream to Kafka topic: '{topic_name}'...")

for current_date in unique_dates:
    daily_batch = df[df['Date'] == current_date]
    
    for index, row in daily_batch.iterrows():
        producer.send(topic_name, value=row.to_dict())
        
    print(f"Sent {len(daily_batch)} records for Date: {current_date}")
    
    time.sleep(1)

producer.flush()
print("All data sent. Stream complete.")
with open("output_sink/.stream_done", "w") as f:
    f.write("done")