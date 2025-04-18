from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

# Define schema for incoming Kafka data
schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ZoneCongestionByMinute") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_data") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_parsed = df_parsed.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))

zone_congestion = (
    df_parsed
    .withWatermark("tpep_pickup_datetime", "1 minutes")
    .groupBy(window(col("tpep_pickup_datetime"),"1 minute"), "PULocationID") \
    .count() \
    .withColumnRenamed("count", "trip_count") \
)

# Output to console
query = zone_congestion.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()