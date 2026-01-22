# LiveKart: Lambda Architecture Analytics Engine 🏎️💨

A full-stack Big Data pipeline implementing the **Lambda Architecture** to process e-commerce transactions. It combines a **Speed Layer** for real-time dashboards and a **Batch Layer** for complex profitability analysis.

## 🏗️ Architecture
This project implements the **Lambda Architecture** pattern:

1.  **Ingestion:** Python Producer generates synthetic order traffic $\rightarrow$ **Apache Kafka**.
2.  **Speed Layer (Real-Time):**
    * **Spark Structured Streaming** reads from Kafka.
    * **Sink 1:** Writes aggregated revenue metrics to **PostgreSQL** (Live Dashboard).
    * **Sink 2:** appends raw data to **Delta Lake (Bronze)** for historical archiving.
3.  **Batch Layer (Historical):**
    * Orchestrated by **Apache Airflow**.
    * **Spark Batch Job** reads Delta Lake (Bronze) + Static Product Catalog (CSV).
    * Calculates **Profit Margins** (Price - Cost) and writes to **PostgreSQL (Analytics Table)**.
4.  **Maintenance:** Automated file compaction tasks managed by Airflow to optimize Lake performance.

## 🛠️ Tech Stack
* **Streaming:** Apache Kafka, Spark Structured Streaming
* **Batch:** Apache Spark (PySpark), Delta Lake 3.0
* **Orchestration:** Apache Airflow 2.7 (DockerOperator)
* **Storage:** PostgreSQL 13 (Serving), Local Data Lake (Storage)
* **Infrastructure:** Docker & Docker Compose

## 🚀 How to Run
1.  **Start Infrastructure:** `docker-compose up -d`
2.  **Initialize Airflow:**
    ```bash
    docker-compose run --rm airflow-webserver airflow db migrate
    docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com
    ```
3.  **Start Producer:** `python scripts/producer.py`
4.  **Start Streaming Job:**
    ```bash
    docker exec -u 0 -it spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.0.0 /opt/spark/scripts/process_stream.py
    ```
5.  **Trigger Batch Job:** Access Airflow at `http://localhost:8090` and run `livekart_lambda_batch`.

## 📊 Key Results
* **Latency:** <1 second for Dashboard updates.
* **Reliability:** Exact-once processing semantics via Checkpointing.
* **Insight:** Combined view of "Gross Revenue" (Real-time) and "Net Profit" (Batch).