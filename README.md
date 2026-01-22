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

## 🚀 Getting Started

### 1. Infrastructure Setup
```powershell
docker-compose up -d
# Initialize Airflow Metadata
docker-compose run --rm airflow-webserver airflow db migrate
# Create Admin User (admin/admin)
docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com