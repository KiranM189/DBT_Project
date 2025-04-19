from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, sum as _sum, count, expr, to_json, struct, to_date
)
from pyspark.sql.types import *
import mysql.connector

# ---------- Clear MySQL Tables ----------
def clear_mysql_tables():
    print(">>> Clearing MySQL tables: raw_trip_data, fare_summary_by_day, avg_speed_fare_by_hour...")
    conn = mysql.connector.connect(
        host="localhost",
        user="spark_user",
        password="spark_pass",
        database="nyc_taxi"
    )
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_trip_data;")
    cursor.execute("DELETE FROM fare_summary_by_day;")
    cursor.execute("DELETE FROM avg_speed_fare_by_hour;")
    conn.commit()
    cursor.close()
    conn.close()
    print(">>> MySQL tables cleared.\n")

clear_mysql_tables()

# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("TaxiDataProcessing") \
    .config("spark.jars", "/home/dee_42/jars/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- Define Schema ----------
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

# ---------- Kafka Source ----------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_raw") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

# ---------- Fill Nulls ----------
numeric_columns = [
    "VendorID", "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee"
]
parsed_df = parsed_df.na.fill(0, subset=numeric_columns)

# ---------- Write Raw Data to MySQL ----------
def write_raw_to_mysql(batch_df, _):
    print(">>> [RAW] Writing to raw_trip_data...")
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/nyc_taxi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "raw_trip_data") \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .mode("append") \
        .save()

raw_query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_raw_to_mysql) \
    .option("checkpointLocation", "/tmp/raw_trip_combined_checkpoint") \
    .start()

# ---------- Daily Fare Aggregation ----------
parsed_df_with_date = parsed_df.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")) \
                               .withColumn("trip_date", to_date("pickup_ts"))

fare_agg_df = parsed_df_with_date.groupBy("trip_date").agg(
    _sum("fare_amount").alias("total_fare"),
    _sum("extra").alias("total_extra"),
    _sum("tip_amount").alias("total_tip"),
    _sum("tolls_amount").alias("total_tolls"),
    _sum("airport_fee").alias("total_airport"),
    _sum("total_amount").alias("total_total")
)

# ---------- Write Aggregated Fare Data to MySQL ----------
def write_agg_to_mysql(batch_df, _):
    if batch_df.isEmpty():
        print("⚠️ [AGGREGATED] Skipped: DataFrame is empty.")
        return
    print("✅ [AGGREGATED] Writing to fare_summary_by_day...")
    batch_df.coalesce(1).write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/nyc_taxi") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "fare_summary_by_day") \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .mode("overwrite") \
        .save()

# ---------- Write Aggregated Fare Data to Kafka ----------
def write_agg_to_kafka(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"⚠️ [KAFKA] Skipped batch {batch_id}: DataFrame is empty.")
        return
    print(f"📤 [KAFKA] Writing batch {batch_id} to taxi_fare_summary topic...")
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

fare_agg_query = fare_agg_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime="1 seconds") \
    .foreachBatch(lambda df, epoch: (write_agg_to_mysql(df, epoch), write_agg_to_kafka(df, epoch))) \
    .option("checkpointLocation", "/tmp/fare_summary_combined_checkpoint") \
    .start()

# ---------- Speed/Fare Aggregation ----------
parsed_df_with_ts = parsed_df.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime")) \
                              .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime"))

filtered_df = parsed_df_with_ts.filter("trip_distance > 0 AND pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL") \
    .withColumn("duration_minutes", (expr("unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)") / 60.0)) \
    .filter("duration_minutes > 0") \
    .withColumn("speed", col("trip_distance") / (col("duration_minutes") / 60.0))

agg_df = filtered_df.withWatermark("pickup_ts", "10 minutes") \
    .groupBy(window("pickup_ts", "1 hour").alias("window")) \
    .agg(
        _sum("fare_amount").alias("total_fare"),
        count("*").alias("trip_count"),
        _sum("trip_distance").alias("total_distance"),
        _sum("duration_minutes").alias("total_minutes")
    ) \
    .withColumn("avg_fare", col("total_fare") / col("trip_count")) \
    .withColumn("avg_speed", col("total_distance") / (col("total_minutes") / 60.0)) \
    .withColumn("start_time", col("window.start")) \
    .withColumn("end_time", col("window.end")) \
    .select("start_time", "end_time", "avg_fare", "avg_speed")

# ---------- Write Speed/Fare to MySQL ----------
def write_avg_to_mysql(df, _):
    if df.isEmpty():
        print("⚠️ [MYSQL] Skipped: DataFrame is empty.")
        return
    print("✅ [MYSQL] Writing average speed/fare to avg_speed_fare_by_hour...")
    df.coalesce(1).write \
      .format("jdbc") \
      .option("url", "jdbc:mysql://localhost:3306/nyc_taxi") \
      .option("driver", "com.mysql.cj.jdbc.Driver") \
      .option("dbtable", "avg_speed_fare_by_hour") \
      .option("user", "spark_user") \
      .option("password", "spark_pass") \
      .mode("append") \
      .save()

# ---------- Write Speed/Fare to Kafka ----------
def write_avg_to_kafka(df, _):
    if df.isEmpty():
        print("⚠️ [KAFKA] Skipped: DataFrame is empty.")
        return
    print("📤 [KAFKA] Writing average speed/fare to Kafka topic: taxi_speed_fare")
    kafka_df = df.select(
        col("start_time").cast("string").alias("key"),
        to_json(struct("start_time", "end_time", "avg_fare", "avg_speed")).alias("value")
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "taxi_speed_fare") \
        .save()

# ---------- Start Streaming ----------
avg_query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch: (write_avg_to_mysql(df, epoch), write_avg_to_kafka(df, epoch))) \
    .option("checkpointLocation", "/tmp/avg_speed_fare_checkpoint") \
    .start()

raw_query.awaitTermination()
fare_agg_query.awaitTermination()
avg_query.awaitTermination()