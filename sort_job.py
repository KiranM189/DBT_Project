from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SortParquetByTimestamp") \
    .getOrCreate()

# Path to the original Parquet file
input_path = "data/yellow_tripdata_2025-01.parquet"
output_path = "data/sorted_tripdata_2025-01.parquet"

# Read the Parquet file
df = spark.read.parquet(input_path)

# Sort by pickup timestamp
sorted_df = df.orderBy("tpep_pickup_datetime")

# Save the sorted data to a new Parquet file
sorted_df.write.mode("overwrite").parquet(output_path)

print("✅ Sorted Parquet file saved to:", output_path)