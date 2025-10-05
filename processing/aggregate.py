from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, avg, desc

# Initialize Spark
spark = SparkSession.builder.appName("AggregateData").getOrCreate()

# Read transformed data
df = spark.read.parquet("data/transformed/yellow_tripdata_clean.parquet")

# Trips per hour
df_hourly = df.withColumn("hour", hour(col("pickup_datetime"))) \
              .groupBy("hour") \
              .count() \
              .orderBy("hour")

df_hourly.show(10)
df_hourly.write.mode("overwrite").parquet("data/processed/yellow_tripdata_hourly.parquet")

# Popular pickup zones
df_pickup = df.groupBy("PULocationID").count().orderBy(desc("count"))
df_pickup.show(10)
df_pickup.write.mode("overwrite").parquet("data/processed/yellow_tripdata_pickup.parquet")

# Popular dropoff zones
df_dropoff = df.groupBy("DOLocationID").count().orderBy(desc("count"))
df_dropoff.show(10)
df_dropoff.write.mode("overwrite").parquet("data/processed/yellow_tripdata_dropoff.parquet")

# Average fare and distance
df_avg = df.agg(
    avg("trip_distance").alias("avg_distance"),
    avg("total_amount").alias("avg_fare")
)
df_avg.show()
df_avg.write.mode("overwrite").parquet("data/processed/yellow_tripdata_avg.parquet")

print("Aggregation completed.")