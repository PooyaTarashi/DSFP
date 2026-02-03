from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("Starting Weather Streaming Job...\n")

# Build SparkSession with Kafka dependency
spark = SparkSession.builder \
    .appName("WeatherStreamingJob") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Session created\n")

# -----------------------------
# Kafka Source
# -----------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather_raw") \
    .option("group.id", "weather_streaming_group") \
    .load()

print("Kafka DataFrame created\n")

# -----------------------------
# Schema for your JSON data
# -----------------------------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("location", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("ingested_at", StringType())
])

# Parse the JSON data from Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add key columns for debugging
debug_df = parsed_df.select(
    col("*")
)

print("Starting streaming query...\n")

# Start the streaming query
query = debug_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-debug-checkpoints") \
    .start()

query.awaitTermination()