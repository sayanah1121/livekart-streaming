from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def calculate_profit():
    spark = SparkSession.builder \
        .appName("LiveKart_Batch_Profit") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    # Paths
    bronze_path = "/opt/airflow/data/lake/bronze"
    catalog_path = "/opt/airflow/data/product_catalog.csv"
    
    print(" Starting Batch Profit Calculation...")
    
    try:
        # 1. Read Raw Streaming Data
        sales_df = spark.read.format("delta").load(bronze_path)
        
        # 2. Read Static Catalog
        catalog_df = spark.read.option("header", "true").csv(catalog_path)
        
        # 3. Join (Enrichment)
        # We assume product_id maps correctly. In a real streaming app, IDs are key.
        # For this demo, let's group by Category since our random IDs might not match perfectly.
        
        # Calculate Revenue per Category from Sales
        sales_summary = sales_df.groupBy("category").agg(_sum("price").alias("total_revenue"))
        
        # Calculate Avg Margin per Category from Catalog
        catalog_df = catalog_df.withColumn("margin", col("supplier_price") / 20) # Fake margin logic
        
        # 4. Complex Transformation (Profit Logic)
        # For demonstration, let's just say Profit = Revenue * 0.2
        final_df = sales_summary.withColumn("estimated_profit", col("total_revenue") * 0.2)
        
        # 5. Write to Postgres (Batch Analytics Table)
        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://livekart_db:5432/livekart_db") \
            .option("dbtable", "batch_profit_analysis") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        print(" Profit Analysis pushed to DB.")
        
    except Exception as e:
        print(f" Error (Stream might be empty): {e}")

if __name__ == "__main__":
    calculate_profit()