import pandas as pd
from kafka import KafkaProducer
import json

# Kafka config
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'taxi_data'

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read first 10 rows from Parquet
df = pd.read_parquet(r'/home/hadoop/dbt_project/data/sorted_tripdata_2025-01.parquet').head(10000)

# Send each row
for _, row in df.iterrows():
    message = row.to_dict()
    # Convert Timestamp to string if needed
    for key, value in message.items():
        if hasattr(value, 'isoformat'):
            message[key] = value.isoformat()
    producer.send(KAFKA_TOPIC, value=message)

producer.flush()
producer.close()

print(f"Sent {len(df)} rows to Kafka topic '{KAFKA_TOPIC}'.")
