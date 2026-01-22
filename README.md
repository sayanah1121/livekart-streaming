# LiveKart: Real-Time E-Commerce Analytics Engine ⚡

A high-throughput streaming pipeline capable of ingesting and analyzing e-commerce transactions with **sub-second latency**. Built with **Apache Kafka** and **Spark Structured Streaming**.



## 🏗️ Architecture
**Producer → Broker → Stream Processor → Serving Layer**

1.  **Traffic Simulation:** Python producer generates synthetic high-volume order data (JSON).
2.  **Message Bus:** **Apache Kafka** buffers events to decouple producers from consumers (Handling Backpressure).
3.  **Stream Processing:** **Spark Structured Streaming** reads from Kafka:
    * Parses complex JSON schemas on the fly.
    * Performs **Stateful Aggregations** (Running Total Revenue per Category).
    * Manages "Exactly-Once" processing semantics.
4.  **Serving Layer:** Uses `foreachBatch` to upsert micro-batches into **PostgreSQL** for real-time dashboarding.

## 🛠️ Tech Stack
* **Messaging:** Apache Kafka 7.4 (Confluent)
* **Processing:** Apache Spark 3.5 (Structured Streaming)
* **Database:** PostgreSQL 13
* **Infrastructure:** Docker Compose (Zookeeper, Broker, Spark Master/Worker)

## 🚀 How to Run
1.  **Start Stack:** `docker-compose up -d`
2.  **Start Producer:** `python scripts/producer.py`
3.  **Submit Job:** ```bash
    docker exec -u 0 -it spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    /opt/spark/process_stream.py
    ```
4.  **View Dashboard:** Query the `real_time_category_stats` table in Postgres.