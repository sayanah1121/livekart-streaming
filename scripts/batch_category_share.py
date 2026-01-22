import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def process():
    print("Starting Category Share Analysis...")
    
    spark = SparkSession.builder \
        .appName("Batch_Category_Share") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        df = spark.read.format("delta").load("/opt/airflow/data/lake/bronze")
        
        total_rev_row = df.agg(_sum("price")).collect()
        if not total_rev_row or total_rev_row[0][0] is None:
             print("No sales data found.")
             return

        total_rev = total_rev_row[0][0]
        cat_df = df.groupBy("category").agg(_sum("price").alias("category_revenue"))
        
        final_df = cat_df.withColumn("market_share_pct", (col("category_revenue") / total_rev) * 100)
        
        print("Writing to Postgres...")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "biz_category_market_share") \
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