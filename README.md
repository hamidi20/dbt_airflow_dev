# 🚀 End-to-End Data Engineering Pipeline (FastAPI + Kafka + dbt)

## 📌 Project Overview

This project demonstrates an end-to-end **data engineering pipeline** built using **FastAPI, Apache Kafka, PostgreSQL, and dbt**.

The architecture follows an event-driven and layered data platform approach, starting from an API service, streaming data through Kafka, storing it in a master database, and transforming it into analytics-ready datasets using dbt and a data mart layer.

---

## 🏗️ Architecture

```text
FastAPI (API Service)
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Kafka Consumer
        ↓
PostgreSQL (Master DB)
        ↓
Foreign Data Wrapper (FDW)
        ↓
PostgreSQL (Data Warehouse)
        ↓
dbt (Transformation Layer)
        ↓
Data Mart Layer
🔄 Data Pipeline Workflow
1️⃣ API Service (FastAPI)
📡 Exposes REST API endpoints
🧾 Receives or generates transactional data
⚡ Acts as the entry point of the system
2️⃣ Kafka Producer
📤 Sends event/data from FastAPI to Kafka
🔄 Decouples API layer from downstream systems
📊 Enables real-time streaming ingestion
3️⃣ Kafka Topic
🧵 Acts as message buffer / event stream
📦 Stores incoming events temporarily
⚡ Supports scalable data flow between services
4️⃣ Kafka Consumer
📥 Reads messages from Kafka topic
🧹 Processes and validates incoming data
💾 Writes data into PostgreSQL Master DB
5️⃣ PostgreSQL (Master DB)
🗄️ Stores raw transactional data (OLTP layer)
📌 Serves as single source of truth
🔒 Maintains operational data integrity
6️⃣ FDW (Foreign Data Wrapper)
🔗 Connects Master DB to Data Warehouse
🌉 Enables cross-database querying
⚡ Avoids unnecessary data duplication
7️⃣ PostgreSQL (Data Warehouse)
📊 Stores analytical and structured datasets
🧱 Used for reporting and transformation layer
🚀 Optimized for read and analytics workloads
8️⃣ dbt (Data Build Tool)
🧪 Transforms raw warehouse data into analytics models
🧹 Implements data cleaning and business logic
🏗️ Builds layered architecture:
staging layer
intermediate layer
mart layer
9️⃣ Data Mart Layer
📊 Final curated dataset for analytics
📈 Optimized for BI tools and reporting
🎯 Source for business insights
🛠️ Technologies Used
🐍 Python 3 (FastAPI, Kafka Consumer/Producer)
⚡ Apache Kafka
🐘 PostgreSQL
🔌 PostgreSQL FDW
🧪 dbt (Data Build Tool)
🐳 Docker & Docker Compose
🗂️ Git & GitHub
👨‍💻 Author

Hamidi
Data Engineer
