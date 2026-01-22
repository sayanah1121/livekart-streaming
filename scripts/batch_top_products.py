import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def process():
    print("Starting Top Products Analysis...")
    
    spark = SparkSession.builder \
        .appName("Batch_Top_Products") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        df = spark.read.format("delta").load("/opt/airflow/data/lake/bronze")
        
        final_df = df.groupBy("product_id", "category") \
            .agg(_sum("quantity").alias("units_sold")) \
            .orderBy(col("units_sold").desc()) \
            .limit(20)
        
        print("Writing to Postgres...")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "biz_top_selling_products") \
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