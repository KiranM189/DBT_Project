import os
import pandas as pd
import json
import time
from kafka import KafkaProducer
from tqdm import tqdm


# --- Configuration ---
DATA_DIR = "data"  # Directory containing your Parquet files
KAFKA_TOPIC = "taxi_raw"
BOOTSTRAP_SERVERS = "localhost:9092"
RECORDS_PER_SECOND = 3  # Stream 10–20 records/sec

# --- Setup Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Get all .parquet files sorted ---
parquet_files = sorted([
    f for f in os.listdir(DATA_DIR)
    if f.endswith(".parquet")
])

if not parquet_files:
    print(f"[❌] No Parquet files found in {DATA_DIR}/")
    exit()

try:
    # --- Stream each file ---
    for parquet_file in parquet_files:
       #file_path = os.path.join(DATA_DIR, parquet_file)
        file_path = "/home/dee_42/DBT/data/sorted_tripdata_2024-01"
        print(f"\n📁 Streaming: {file_path}")

        df = pd.read_parquet(file_path)
        df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(str)
        df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(str)
        print(f"🔹 Records: {len(df)}")

        for i, row in tqdm(enumerate(df.itertuples(index=False)), total=len(df)):
            record = {col: getattr(row, col) for col in df.columns if pd.notna(getattr(row, col))}
            producer.send(KAFKA_TOPIC, value=record)

            if (i + 1) % RECORDS_PER_SECOND == 0:
                time.sleep(1)

except KeyboardInterrupt:
    print("\n⛔ Interrupted by user.")
finally:
    producer.flush()
    print("✅ Producer flushed.")
