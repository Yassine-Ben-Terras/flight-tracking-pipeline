# ✈️ Real-Time Global Flight Tracking Pipeline

An end-to-end, production-style data engineering pipeline designed to ingest, process, and analyze real-time global flight telemetry data.

This project streams live state vectors from the OpenSky Network, processes them through a distributed architecture, and structures the data for scalable analytics and machine learning use cases.

---

##  Project Overview

The goal of this project is to simulate a real-world streaming data platform by combining modern data engineering tools into a cohesive architecture.

It demonstrates:

* Real-time data ingestion
* Stream processing at scale
* NoSQL data modeling for time-series workloads
* Analytical transformations for downstream consumption

---

##  Architecture & Tech Stack

The pipeline is fully containerized and built around a streaming-first architecture:

* **Data Source:** OpenSky Network REST API (live flight state vectors)
* **Ingestion Layer:** Apache Kafka + Schema Registry (Avro serialization)
* **Processing Layer:** Apache Spark (Structured Streaming / PySpark)
* **Storage Layer:** Apache Cassandra (optimized for high-throughput time-series data)
* **Analytics Layer:** dbt (data transformation and modeling)
* **Infrastructure:** Docker Compose (local orchestration)

---

##  High-Level Architecture

```
OpenSky API → Kafka → Spark Streaming → Cassandra → dbt → Analytics / ML
```

---

##  Repository Structure

```
.
├── docker-compose.yml     # Infrastructure orchestration
├── cassandra/             # CQL schema definitions & initialization scripts
├── producers/             # Kafka producers (OpenSky ingestion)
├── spark-jobs/            # PySpark structured streaming jobs
├── dbt/                   # Data transformation models (staging → marts)
└── schemas/               # Avro schemas for Kafka messages
```

---

##  Getting Started

### 1. Start the Infrastructure

Ensure Docker is running, then launch all services:

```bash
docker compose up -d
```

This will start:

* Zookeeper
* Kafka
* Schema Registry
* Cassandra
* Spark (Master + Worker)

---

### 2. Initialize Cassandra

Load the database schema:

```bash
docker exec -i flight-tracking-pipeline-cassandra-1 cqlsh < cassandra/init.cql
```

---

### 3. Set Up Python Environment

Prepare the ingestion service:

```bash
python -m venv venv
source venv/bin/activate   # Windows: .\venv\Scripts\activate
pip install -r producers/requirements.txt
```

---

### 4. Create Kafka Topic

```bash
docker exec -it flight-tracking-pipeline-kafka-1 kafka-topics --create --topic flight-states --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

---

### 5. Run the Pipeline

* Start the Kafka producer (OpenSky ingestion)
* Launch Spark streaming jobs
* Monitor data flowing into Cassandra

---

##  Data Modeling Strategy

The Cassandra schema is designed for high-performance, query-driven access patterns:

* **`flight_states`** — Real-time flight telemetry (partitioned by `icao24`)
* **`airport_traffic`** — Aggregated airport activity (partitioned by airport + time window)
* **`flight_routes`** — Historical route data for trend analysis

---

##  Analytics Layer (dbt)

The dbt layer transforms raw streaming data into structured analytical models:

* **Staging Models:** Data cleaning and normalization
* **Intermediate Models:** Business logic and aggregations
* **Mart Models:** KPI-ready datasets

Example use cases:

* Airport congestion analysis
* Flight volume trends
* Route popularity insights
* Feature engineering for ML models

---

##  Key Engineering Highlights

* Event-driven architecture using Kafka
* Schema enforcement with Avro + Schema Registry
* Stateful stream processing with Spark Structured Streaming
* Query-first data modeling in Cassandra
* Modular and reproducible environment via Docker

---

##  Future Improvements

* Add real-time dashboards (Superset / Grafana)
* Implement anomaly detection on flight patterns
* Deploy to cloud (AWS / GCP)
* Introduce CI/CD for pipeline automation
