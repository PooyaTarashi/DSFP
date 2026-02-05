from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

print("Starting Weather Streaming Job ...\n\n")

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("WeatherStreamingJob") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ I'm here 1...\n\n\n\n")

# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather_raw") \
    .option("startingOffsets", "latest") \
    .load()

print("✅ I'm here 2...\n\n\n\n")

# --------------------------------------------------
# Schema (CORRECTED to match Kafka)
# --------------------------------------------------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("ingested_at", StringType())
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --------------------------------------------------
# Event Time + Location Column
# --------------------------------------------------
weather_df = parsed_df \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withColumn("location", col("city"))

print("✅ I'm here 3...\n\n\n\n")

# ==================================================
# 1️⃣ RAW DATA → HDFS
# ==================================================
raw_query = weather_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://hdfs-namenode:9005/weather/raw"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://hdfs-namenode:9005/weather/checkpoints/raw"
    ) \
    .partitionBy("location") \
    .start()

print("✅ I'm here 4...\n\n\n\n")

# ==================================================
# 2️⃣ AGGREGATED DATA → HDFS
# ==================================================
agg_df = weather_df \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("location")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("records_count")
    )

print("✅ I'm here 5...\n\n\n\n")

agg_query = agg_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://hdfs-namenode:9005/weather/aggregated"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://hdfs-namenode:9005/weather/checkpoints/aggregated"
    ) \
    .partitionBy("location") \
    .start()

print("✅ I'm here 6...\n\n\n\n")

# --------------------------------------------------
# Await
# --------------------------------------------------
spark.streams.awaitAnyTermination()