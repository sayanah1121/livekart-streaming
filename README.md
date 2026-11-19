# LiveKart: High-Throughput E-Commerce Data Platform (Lambda Architecture)

LiveKart is a professional-grade Data Engineering project implementing a **Lambda Architecture**. It handles high-frequency e-commerce transaction streams, providing both real-time ingestion and deep-dive batch analytics.

## 🏗️ Architecture Overview
The system is built to handle the "3 Vs" of data (Volume, Velocity, Variety):

1.  **Producer Layer:** Python-based simulation generating rich JSON transaction data (Order IDs, User IDs, Product IDs, Categories, and Quantities).
2.  **Streaming Layer (Speed):** **Apache Kafka** acts as the message broker, while **Spark Structured Streaming** ingests the data in real-time.
3.  **Storage Layer:** Data is persisted in **Delta Lake** (Bronze Layer), providing ACID transactions and schema enforcement.
4.  **Orchestration Layer:** **Apache Airflow** manages the workflow, running a sequential pipeline of Spark Batch jobs.
5.  **Analytics Layer (Batch):** 6 specialized Spark jobs process historical data to derive complex business KPIs.
6.  **Serving Layer:** Processed insights are pushed via JDBC to a **PostgreSQL** Data Warehouse for BI consumption.

## 📊 Business Intelligence Modules
The platform automatically calculates and updates the following metrics:
* **Customer Segmentation:** Classifies users into Platinum, Gold, and Silver tiers based on lifetime spend.
* **Supplier Performance:** Tracks net profit margins by joining sales data with supplier catalogs.
* **Market Share Analysis:** Calculates real-time revenue percentage breakdown across all product categories.
* **Hourly Sales Velocity:** Identifies peak traffic hours to optimize infrastructure and marketing spend.
* **Top-Selling Products:** Ranks the top 20 products by unit volume across the entire platform.
* **Profit Margin Analytics:** Detailed gap analysis between retail pricing and supplier costs.

## 🛠️ Tech Stack
* **Languages:** Python, SQL, Scala/Java (Spark Core)
* **Streaming:** Apache Kafka
* **Data Processing:** Apache Spark (Structured Streaming + Batch)
* **Table Format:** Delta Lake (ACID compliant)
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL
* **Containerization:** Docker & Docker-Compose

## 📏 Measured Performance

Measured on 2026-11-19 on a laptop: Windows 11, Docker Desktop (WSL2) with 12 vCPUs and 8 GB RAM. Kafka has 1 broker and 1 partition, and Spark has 1 master and 1 worker. Each number is from a single run, so treat them as a local baseline, not a cluster benchmark.

### Throughput (1,000,000 events)

| Stage | Result |
|---|---|
| Producer → Kafka (`producer.py --rate 0`, one Python process) | **10,320 events/s** (1M events in 96.9 s) |
| Kafka → Spark Structured Streaming → Delta Bronze (100k events per micro-batch) | **11,300 rows/s** steady state (9 batches, 9.8k–12.9k rows/s), 1M rows in 112.6 s of trigger time |
| First micro-batch (JVM and Delta warm-up) | 3,090 rows/s (32.4 s for 100k rows) |

Spark writes its per-batch progress metrics to `data/metrics/stream_progress.jsonl`.

### Rows per layer

| Layer | Table | Rows |
|---|---|---|
| Kafka | `livekart_orders` topic | 1,000,000 |
| Bronze (Delta) | `data/lake/bronze` | 1,000,000 (10 Parquet files, 13 MB) |
| Serving (Postgres) | `biz_user_segments` | 100 |
| | `biz_top_selling_products` | 20 |
| | `biz_supplier_performance` | 11 |
| | `biz_category_market_share` | 5 |
| | `batch_profit_analysis` | 5 |
| | `biz_hourly_sales_velocity` | 1 |

### Batch runtime (6 Spark jobs run one after another over 1M Bronze rows, as in the Airflow DAG)

| Job | Wall time | Spark job time |
|---|---|---|
| `batch_profit_calc` | 29.3 s | 2.3 s |
| `batch_user_segmentation` | 33.7 s | 3.0 s |
| `batch_supplier_perf` | 30.7 s | 2.1 s |
| `batch_category_share` | 33.5 s | 2.5 s |
| `batch_hourly_velocity` | 35.2 s | 2.0 s |
| `batch_top_products` | 34.1 s | 2.3 s |
| **Total (end-to-end)** | **199.5 s (3 min 19 s)** | **14.2 s** |

Each job starts in its own Spark container, like the DAG's `DockerOperator`, so container and JVM startup take about 90% of the wall time. The Spark work itself takes 2–3 s per job. These times use a warm Ivy jar cache. Without the cache, every task downloads the Delta and Postgres jars from Maven first.

**What the numbers show:**
* Every user ends up Platinum. 1M orders across 100 users is about $2.6M of spend each, far above the $5,000 Platinum cutoff.
* Hourly velocity has only 1 row, because all 1M events were produced within the same hour.
* `biz_supplier_performance` has an extra NULL-supplier row. The producer emits a `Toys` category, but the product catalog has no Toys rows.
* `batch_profit_calc` catches its own exceptions, so it exits 0 even when it fails.

### Reproduce
```powershell
docker compose up -d zookeeper kafka postgres spark-master spark-worker
python scripts/producer.py --rate 0 --count 1000000 --quiet
docker exec -e LIVEKART_AVAILABLE_NOW=1 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 /opt/spark/scripts/process_stream.py
```
Then run each `scripts/batch_*.py` with the `spark-submit` command from `dags/livekart_analytics_dag.py`. Use `--rate N` to send at a fixed rate. The default is 1 event/s, which is the original behaviour.

## 🚀 Getting Started

### 1. Infrastructure Setup
```powershell
docker-compose up -d
# Initialize Airflow Metadata
docker-compose run --rm airflow-webserver airflow db migrate
# Create Admin User (admin/admin)
docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com