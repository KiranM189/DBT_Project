import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import round as _round, to_json, struct
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, sum as _sum
from pyspark.sql.types import *

# ---------- Clear MySQL Tables First ----------
def clear_mysql_tables():
    print(">>> Clearing MySQL tables: raw_trip_data and fare_summary_by_day...")
    conn = mysql.connector.connect(
        host="localhost",
        user="spark_user",
        password="spark_pass",
        database="nyc_taxi"
    )
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_trip_data;")
    cursor.execute("DELETE FROM fare_summary_by_day;")
    conn.commit()
    cursor.close()
    conn.close()
    print(">>> MySQL tables cleared.\n")

clear_mysql_tables()

# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("CombinedRawAndDailyFareToMySQL") \
    .config("spark.jars", "/home/dee_42/jars/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- Define Kafka Source ----------
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_raw") \
    .option("startingOffsets", "latest") \
    .load()

# ---------- Schema ----------
schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType())
])

# ---------- Parse JSON ----------
parsed_df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ---------- Fill nulls ----------
numeric_columns = [
    "VendorID", "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee"
]
parsed_df = parsed_df.na.fill(0, subset=numeric_columns)

# ---------- RAW Data Writer ----------
def write_raw_to_mysql(batch_df, _):
    print(">>> [RAW] Writing to raw_trip_data...")
    batch_df.select("tpep_pickup_datetime", "fare_amount").show(5, False)
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/nyc_taxi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "raw_trip_data") \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .mode("append") \
        .save()

# ---------- Aggregation ----------
parsed_df_with_date = parsed_df.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")) \
                               .withColumn("trip_date", to_date("pickup_ts"))

agg_df = parsed_df_with_date.groupBy("trip_date").agg(
    _round(_sum("fare_amount"), 2).alias("total_fare"),
    _round(_sum("extra"), 2).alias("total_extra"),
    _round(_sum("tip_amount"), 2).alias("total_tip"),
    _round(_sum("tolls_amount"), 2).alias("total_tolls"),
    _round(_sum("airport_fee"), 2).alias("total_airport"),
    _round(_sum("total_amount"), 2).alias("total_total")
)

# ---------- Aggregated Writer to MySQL ----------
def write_agg_to_mysql(batch_df, _):
    print(">>> [AGGREGATED] Writing to fare_summary_by_day...")
    batch_df.show(truncate=False)
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/nyc_taxi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "fare_summary_by_day") \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .mode("overwrite") \
        .save()

# ---------- Aggregated Writer to Kafka (with Print) ----------
def write_agg_to_kafka(batch_df, batch_id):
    print(f">>> [KAFKA] Writing batch {batch_id} to taxi_fare_summary topic...")
    kafka_ready_df = batch_df.select(
        col("trip_date").cast("string").alias("key"),
        to_json(struct(
            col("trip_date"),
            col("total_fare"),
            col("total_extra"),
            col("total_tip"),
            col("total_tolls"),
            col("total_airport"),
            col("total_total")
        )).alias("value")
    )
    kafka_ready_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "taxi_fare_summary") \
        .save()

# ---------- Start Raw Stream ----------
raw_query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_raw_to_mysql) \
    .option("checkpointLocation", "/tmp/raw_trip_combined_checkpoint") \
    .start()

# ---------- Start Aggregation Stream to MySQL ----------
agg_query = agg_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime="3 seconds") \
    .foreachBatch(write_agg_to_mysql) \
    .option("checkpointLocation", "/tmp/fare_summary_combined_checkpoint") \
    .start()

# ---------- Start Aggregation Stream to Kafka ----------
agg_to_kafka_query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_agg_to_kafka) \
    .option("checkpointLocation", "/tmp/fare_summary_kafka_checkpoint") \
    .start()

# ---------- Await ----------
raw_query.awaitTermination()
agg_query.awaitTermination()
agg_to_kafka_query.awaitTermination()
