from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

def process_stream():
    spark = SparkSession.builder \
        .appName("LiveKart_Streaming_Lambda") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Schema
    schema = StructType() \
        .add("order_id", StringType()) \
        .add("user_id", StringType()) \
        .add("product_id", StringType()) \
        .add("category", StringType()) \
        .add("price", FloatType()) \
        .add("quantity", IntegerType()) \
        .add("timestamp", StringType())

    # Read Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "livekart_orders") \
        .load()

    # Parse JSON
    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Enrich with timestamp
    processed_df = json_df.withColumn("ingestion_time", current_timestamp())

    # --- SINK 1: REAL-TIME DASHBOARD (Postgres) ---
    def write_to_postgres(batch_df, batch_id):
        # Aggregation for Dashboard
        agg_df = batch_df.groupBy("category").agg({"price": "sum"}).withColumnRenamed("sum(price)", "total_revenue")
        
        agg_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "real_time_category_stats") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
    # --- SINK 2: DATA LAKE ARCHIVE (Bronze Layer for Airflow) ---
    # We use 'append' mode to Parquet/Delta for historical analysis
    file_query = processed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/bronze") \
        .option("path", "/opt/spark/data/lake/bronze") \
        .start()

    # Start Postgres Stream
    db_query = processed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()