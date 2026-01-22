import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

def process():
    print("Starting User Segmentation Analysis...")
    
    spark = SparkSession.builder \
        .appName("Batch_User_Segmentation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        print("Reading Delta Table...")
        df = spark.read.format("delta").load("/opt/airflow/data/lake/bronze")
        
        if df.isEmpty():
            print("WARNING: Bronze Lake is empty. Skipping analysis.")
            return

        user_spend = df.groupBy("user_id").agg(_sum("price").alias("total_lifetime_spend"))
        
        final_df = user_spend.withColumn("segment", 
            when(col("total_lifetime_spend") > 5000, "Platinum")
            .when(col("total_lifetime_spend") > 2000, "Gold")
            .when(col("total_lifetime_spend") > 500, "Silver")
            .otherwise("Bronze")
        )
        
        print("Writing to Postgres...")
        final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "biz_user_segments") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        print("Success!")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    process()