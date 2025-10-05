from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BatchIngest").getOrCreate()

# Read Parquet
df = spark.read.parquet("data/raw/yellow_tripdata.parquet")

# Save to processed folder
df.write.mode("overwrite").parquet("data/processed/yellow_tripdata_raw.parquet")

print("Batch ingestion completed. Rows:", df.count())