# Real-Time Global Flight Tracking Pipeline

An end-to-end, production-style data engineering pipeline designed to ingest, process, and analyze real-time global flight telemetry data.

This project implements a hybrid architecture, separating low-latency streaming processes from scheduled batch analytics. It streams live state vectors from the OpenSky Network, processes them through a distributed architecture, and structures the data for real-time visualization and downstream ML use cases.

---

## Project Overview

The goal of this project is to simulate a real-world streaming data platform by combining modern data engineering tools into a cohesive architecture.

It demonstrates:
- Real-time data ingestion
- Stream processing at scale (dual-write architecture)
- Relational modeling for current state and NoSQL for time-series workloads
- Automated analytical transformations (batch processing)
- Secure data serving and real-time visualization

---

## Architecture & Tech Stack

The pipeline is fully containerized and built around a streaming-first, hybrid architecture:

- Data Source: OpenSky Network REST API (live flight state vectors)
- Ingestion Layer: Apache Kafka + Schema Registry
- Processing Layer: Apache Spark (Structured Streaming / PySpark)
- Storage Layer:
  - PostgreSQL: Relational data warehouse for current state and metadata
  - Apache Cassandra: Optimized NoSQL storage for high-throughput historical trajectories
- Orchestration & Analytics Layer: Apache Airflow + dbt (batch data transformation on PostgreSQL)
- Serving Layer: FastAPI (REST API for secure data access and business logic)
- Visualization Layer: Grafana (real-time dashboards, geomaps, and KPIs)
- Infrastructure: Docker Compose (local orchestration)

---

## High-Level Architecture

```
OpenSky API → Kafka → Spark Streaming ┬→ PostgreSQL (Current State) ──→ dbt (Airflow Batch) ┬→ FastAPI → Grafana
                                      └→ Cassandra (Historical Logs) ───────────────────────┘
```

---

## Repository Structure

```
.
├── docker-compose.yml
├── dags/
├── api/
├── cassandra/
├── producers/
├── spark-jobs/
├── dbt_project/
└── schemas/
```

---

## Getting Started

### 1. Prepare the Environment

Ensure Docker is running. Before launching Airflow, create the required directories and set permissions:

```bash
mkdir -p dags logs plugins
chmod -R 777 dags logs plugins
```

### 2. Start the Infrastructure

```bash
docker compose up -d
```

This will start Kafka, Schema Registry, Cassandra, PostgreSQL, Spark Cluster, Airflow, FastAPI, and Grafana.

### 3. Initialize Databases

```bash
docker exec -i flight-tracking-pipeline-cassandra-1 cqlsh < cassandra/init.cql
```

### 4. Set Up Python Environment (Producer)

```bash
python -m venv venv
source venv/bin/activate   # Windows: .\venv\Scripts\activate
pip install -r producers/requirements.txt
```

### 5. Create Kafka Topic

```bash
docker exec -it flight-tracking-pipeline-kafka-1 kafka-topics --create --topic flight-states --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

### 6. Run the Pipeline

- Start the Kafka producer:
```bash
python producers/opensky_ingestion.py
```

- Launch Spark streaming jobs to consume from Kafka and write to PostgreSQL and Cassandra
- Airflow will automatically trigger dbt models on schedule

---

## Accessing the Interfaces

- Grafana: http://localhost:3000  
- Apache Airflow: http://localhost:8080  
- FastAPI (Swagger UI): http://localhost:8000/docs  

---

## Data Modeling Strategy

The pipeline uses a dual-database approach to optimize query performance:

PostgreSQL (managed by dbt):
- fact_flight_movements — real-time current positions and speeds
- dim_aircraft / dim_airlines — metadata and dimension tables
- Aggregated marts — daily KPIs, active flight counts, airport congestion metrics

Apache Cassandra (time-series):
- flight_states — append-only historical telemetry (partitioned by icao24)
- flight_routes — historical route data for ML training and trend analysis

---

## Key Engineering Highlights

- Hybrid pipeline: separation between continuous streaming (Spark) and scheduled batch analytics (Airflow + dbt)
- Dual-write processing: writes to both PostgreSQL and Cassandra for different workloads
- API serving layer: FastAPI decouples data access from visualization
- Modular infrastructure: fully reproducible via Docker Compose

---

## Future Improvements

- Implement anomaly detection on flight patterns using MLflow
- Deploy to cloud platforms (AWS / GCP) using Terraform
- Add CI/CD pipelines (GitHub Actions) for testing and deployment