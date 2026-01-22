import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def process():
    print("Starting Supplier Performance Analysis...")
    
    spark = SparkSession.builder \
        .appName("Batch_Supplier_Performance") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        sales_df = spark.read.format("delta").load("/opt/airflow/data/lake/bronze")
        catalog_df = spark.read.option("header", "true").csv("/opt/airflow/data/product_catalog.csv")
        
        joined_df = sales_df.join(catalog_df, "category", "left")
        profit_df = joined_df.withColumn("net_profit", col("price") - col("supplier_price"))
        
        final_df = profit_df.groupBy("supplier_name") \
            .agg(_sum("net_profit").alias("total_profit_generated")) \
            .orderBy(col("total_profit_generated").desc())
        
        print("Writing to Postgres...")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "biz_supplier_performance") \
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