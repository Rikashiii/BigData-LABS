from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark Session
spark = SparkSession.builder \
    .appName("UberRidesStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Set log level to reduce console output
spark.sparkContext.setLogLevel("WARN")

# Define schema for the CSV file
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("drop_location", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])

# Read streaming data from directory
streaming_df = spark.readStream \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema) \
    .csv("file:///tmp/uber_data")

print("Schema loaded successfully")

# Computation 1: Average fare_amount by vehicle_type
avg_fare_by_vehicle = streaming_df \
    .groupBy("vehicle_type") \
    .agg(avg("fare_amount").alias("avg_fare_amount"))

# Computation 2: Total rides per payment_type
total_rides_by_payment = streaming_df \
    .groupBy("payment_type") \
    .agg(count("ride_id").alias("total_rides"))

print("\n=== Starting Streaming Queries ===")
print("Monitoring directory: /tmp/uber_data")
print("Trigger interval: 30 seconds")
print("Waiting for CSV files to arrive...\n")

# Console output for Query 1: Average fare by vehicle type
console_query1 = avg_fare_by_vehicle.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .queryName("AvgFareByVehicle") \
    .start()

print("Query 1 started: Average Fare by Vehicle Type (Console)")

# Console output for Query 2: Total rides by payment type
console_query2 = total_rides_by_payment.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .queryName("TotalRidesByPayment") \
    .start()

print("Query 2 started: Total Rides by Payment Type (Console)")

# For Parquet output, we need to use the original streaming_df
# and write it in append mode, then we can aggregate later

# Write raw data to parquet for avg_fare calculation
parquet_query1 = streaming_df \
    .select("vehicle_type", "fare_amount") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/tmp/streaming_output/rides_by_vehicle") \
    .option("checkpointLocation", "/tmp/checkpoint/rides_vehicle") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Parquet Query 1 started: Rides by Vehicle (Raw data for aggregation)")

# Write raw data to parquet for payment type calculation
parquet_query2 = streaming_df \
    .select("payment_type", "ride_id") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/tmp/streaming_output/rides_by_payment") \
    .option("checkpointLocation", "/tmp/checkpoint/rides_payment") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Parquet Query 2 started: Rides by Payment (Raw data for aggregation)")

print("\nAll queries running. Add CSV files to /tmp/uber_data to see results.")
print("Press Ctrl+C to stop.\n")

# Wait for all queries to terminate
spark.streams.awaitAnyTermination()
