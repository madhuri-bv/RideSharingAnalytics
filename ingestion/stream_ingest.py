import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as _sum, lag, row_number, round, when
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StreamingIngest").getOrCreate()

# -----------------------------
# Load clean data
# -----------------------------
df = spark.read.parquet("data/transformed/yellow_tripdata_clean.parquet")

# For test mode: limit rows
TEST_MODE = True
MAX_ROWS = 5000
if TEST_MODE:
    df = df.limit(MAX_ROWS)

# -----------------------------
# Fix ambiguous column
# -----------------------------
if "pickup_datetime" in df.columns:
    df = df.drop("pickup_datetime")  # remove duplicate if exists

if "tpep_pickup_datetime" in df.columns:
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")

# Add year & month columns
df = df.withColumn("year", year(col("pickup_datetime"))) \
       .withColumn("month", month(col("pickup_datetime")))

# -----------------------------
# Simulate streaming by batch processing
# -----------------------------
BATCH_SIZE = 500
total_rows = df.count()
start = 0

while start < total_rows:
    end = start + BATCH_SIZE
    batch_df = df.limit(end).subtract(df.limit(start))
    if batch_df.count() == 0:
        break

    # -----------------------------
    # Monthly trips & MoM growth
    # -----------------------------
    monthly_trips = batch_df.groupBy("year", "month").count().orderBy("year", "month")
    window = Window.orderBy("year", "month")
    monthly_trips = monthly_trips.withColumn("prev_count", lag("count").over(window))
    monthly_trips = monthly_trips.withColumn(
        "mom_growth_pct",
        when(col("prev_count").isNotNull(), round((col("count") - col("prev_count")) / col("prev_count") * 100, 2))
    )
    monthly_trips.write.mode("append").parquet("data/processed/stream_monthly_trips")

    # -----------------------------
    # Monthly revenue & MoM growth
    # -----------------------------
    monthly_revenue = batch_df.groupBy("year", "month").agg(_sum("total_amount").alias("total_revenue")).orderBy("year", "month")
    monthly_revenue = monthly_revenue.withColumn("prev_revenue", lag("total_revenue").over(window))
    monthly_revenue = monthly_revenue.withColumn(
        "revenue_growth_pct",
        when(col("prev_revenue").isNotNull(), round((col("total_revenue") - col("prev_revenue")) / col("prev_revenue") * 100, 2))
    )
    monthly_revenue.write.mode("append").parquet("data/processed/stream_monthly_revenue")

    # -----------------------------
    # Top pickup zones per month
    # -----------------------------
    top_pickups = batch_df.groupBy("year","month","PULocationID").count()
    rank_window = Window.partitionBy("year","month").orderBy(col("count").desc())
    top_pickups = top_pickups.withColumn("rank", row_number().over(rank_window)).filter(col("rank") <= 5)
    top_pickups.write.mode("append").parquet("data/processed/stream_top_pickups")

    # -----------------------------
    # Cumulative distance per month
    # -----------------------------
    cum_window = Window.partitionBy("year","month").orderBy("pickup_datetime").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    batch_df = batch_df.withColumn("cumulative_distance", _sum("trip_distance").over(cum_window))
    batch_df.select("year","month","pickup_datetime","trip_distance","cumulative_distance") \
            .write.mode("append").parquet("data/processed/stream_cum_distance")

    start += BATCH_SIZE
    time.sleep(1)  # simulate streaming

print("Streaming ingestion & transformations completed successfully.")