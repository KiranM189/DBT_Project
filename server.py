from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'taxi_data'


# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # or 'latest'
    enable_auto_commit=True
)

print(f"Listening to topic '{KAFKA_TOPIC}'...")

# Consume messages
for message in consumer:
    print("Received message:")
    print(message.value)
