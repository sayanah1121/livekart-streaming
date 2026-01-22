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
        .load()

    # Parse JSON and add ingestion timestamp
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    final_df = parsed_df.withColumn("ingestion_time", current_timestamp())

    print(" Speed Layer active: Writing to Delta Lake...")

    # Write to Delta Lake (Bronze Layer)
    query = final_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/bronze") \
        .start("/opt/spark/data/lake/bronze")

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()