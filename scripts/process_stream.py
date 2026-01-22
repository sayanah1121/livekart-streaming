import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- CONFIGURATION ---
# Note: Spark runs INSIDE Docker, so it talks to 'livekart_db' (Container Name) on port 5432
DB_URL = "jdbc:postgresql://livekart_db:5432/livekart_db"
DB_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("LiveKart_RealTime_Analytics") \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    """
    This function runs for every single micro-batch.
    It takes the current state of the data and overwrites the SQL table.
    """
    print(f"Write Batch {epoch_id} to Database...")
    try:
        # We use mode("overwrite") because 'Complete' mode gives us the WHOLE table every time.
        # We replace the old dashboard data with the fresh numbers.
        df.write.jdbc(url=DB_URL, table="real_time_category_stats", mode="overwrite", properties=DB_PROPERTIES)
    except Exception as e:
        print(f" Database Write Failed: {e}")

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("⏳ Starting Real-Time Stream to PostgreSQL...")

    # 1. Define Schema
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("user_id", StringType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", LongType()),
        StructField("payment_method", StringType())
    ])

    # 2. Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "livekart_orders") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse Data
    clean_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 4. Aggregate (Live Dashboard Logic)
    category_stats = clean_stream.groupBy("category") \
        .agg(
            sum("price").alias("total_revenue"),
            count("order_id").alias("order_count")
        )

    # 5. Write to Database (The Change)
    # outputMode("complete"): "Give me the entire updated table"
    # foreachBatch: "Pass that table to our custom function to write to SQL"
    query = category_stats.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()