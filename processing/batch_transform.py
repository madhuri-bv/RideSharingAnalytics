from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, count, avg

spark = SparkSession.builder.appName("BatchTransform").getOrCreate()

# -----------------------------
# Load raw data
# -----------------------------
df = spark.read.parquet("data/raw/yellow_tripdata.parquet")

# -----------------------------
# Hourly trips
# -----------------------------
df = df.withColumn("hour", hour(col("tpep_pickup_datetime")))
hourly_trips = df.groupBy("hour").count().orderBy("hour")
hourly_trips.write.mode("overwrite").parquet("data/processed/batch_hourly")

# -----------------------------
# Top pickup zones
# -----------------------------
pickup_zones = df.groupBy("PULocationID").count().orderBy(col("count").desc())
pickup_zones.write.mode("overwrite").parquet("data/processed/batch_pickup")

# -----------------------------
# Top dropoff zones
# -----------------------------
dropoff_zones = df.groupBy("DOLocationID").count().orderBy(col("count").desc())
dropoff_zones.write.mode("overwrite").parquet("data/processed/batch_dropoff")

# -----------------------------
# Average fare & distance per pickup location
# -----------------------------
avg_metrics = df.groupBy("PULocationID").agg(
    avg("fare_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance")
)
avg_metrics.write.mode("overwrite").parquet("data/processed/batch_avg")

print("Batch transformations completed successfully.")