import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, count

def process():
    print("Starting Hourly Velocity Analysis...")
    
    spark = SparkSession.builder \
        .appName("Batch_Hourly_Velocity") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        df = spark.read.format("delta").load("/opt/airflow/data/lake/bronze")
        
        time_df = df.withColumn("sale_hour", hour(from_unixtime(col("timestamp"))))
        
        final_df = time_df.groupBy("sale_hour") \
            .agg(count("order_id").alias("total_orders")) \
            .orderBy("sale_hour")
        
        print("Writing to Postgres...")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "biz_hourly_sales_velocity") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    process()