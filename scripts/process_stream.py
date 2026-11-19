import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def process_stream():
    # HARDCODED DELTA CONFIGURATIONS
    spark = SparkSession.builder \
        .appName("LiveKart_Streaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark/data/checkpoints") \
        .getOrCreate()

    # Schema including user_id, product_id, and quantity
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "livekart_orders") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", os.environ.get("LIVEKART_MAX_OFFSETS", "100000")) \
        .load()

    # Parse JSON and add ingestion timestamp
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    final_df = parsed_df.withColumn("ingestion_time", current_timestamp())

    print(" Speed Layer active: Writing to Delta Lake...")

    # Write to Delta Lake (Bronze Layer)
    writer = final_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/bronze")

    # LIVEKART_AVAILABLE_NOW=1 drains the current Kafka backlog and exits (used for throughput benchmarks)
    if os.environ.get("LIVEKART_AVAILABLE_NOW") == "1":
        writer = writer.trigger(availableNow=True)

    query = writer.start("/opt/spark/data/lake/bronze")

    # Log Spark's per-micro-batch progress (rows, rows/sec, durations) for benchmarking
    metrics_path = os.environ.get("LIVEKART_METRICS", "/opt/spark/data/metrics/stream_progress.jsonl")
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    seen = set()
    with open(metrics_path, "a") as out:
        while query.isActive:
            query.awaitTermination(5)
            for p in query.recentProgress:
                if p["batchId"] not in seen and p["numInputRows"] > 0:
                    seen.add(p["batchId"])
                    out.write(json.dumps({
                        "runId": p["runId"],
                        "batchId": p["batchId"],
                        "timestamp": p["timestamp"],
                        "numInputRows": p["numInputRows"],
                        "inputRowsPerSecond": p.get("inputRowsPerSecond"),
                        "processedRowsPerSecond": p.get("processedRowsPerSecond"),
                        "durationMs": p["durationMs"],
                    }) + "\n")
                    out.flush()
    if query.exception():
        raise query.exception()

if __name__ == "__main__":
    process_stream()