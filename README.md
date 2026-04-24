# ✈️ Real-Time Global Flight Tracking Pipeline

An end-to-end, production-grade ELT (Extract, Load, Transform) pipeline designed to ingest, process, and analyze real-time global flight telemetry data.

Developed within the context of advanced studies in Intelligent Processing Systems, this project implements a hybrid architecture that separates low-latency streaming processes from scheduled batch analytics. It streams live state vectors from the OpenSky Network, processes them through a distributed system, and structures the data for real-time visualization and downstream analytics.

---

##  Project Overview

This repository simulates a real-world streaming data platform by integrating modern data engineering tools into a cohesive and scalable architecture.

### Key Capabilities
- **Real-Time Data Ingestion:** Captures live flight telemetry via REST APIs.
- **Scalable Stream Processing:** Implements a dual-write architecture across relational and NoSQL systems.
- **Modern ELT Workflow:** Loads raw data before transformation using dbt.
- **Automated Orchestration:** Executes container-native workflows with Apache Airflow.
- **Data Visualization:** Provides real-time dashboards connected to curated data models.

---

##  Architecture & Tech Stack

The pipeline is fully containerized and built around a streaming-first, hybrid ELT architecture.

### Core Components

- **Data Source:** OpenSky Network REST API (live flight state vectors)
- **Ingestion Layer:** Apache Kafka + Zookeeper
- **Processing Layer:** Apache Spark (Structured Streaming / PySpark)

### Storage Layer (Dual-Write Strategy)
- **PostgreSQL:** Relational warehouse for current state and dimensional models (`flight_warehouse`)
- **Apache Cassandra:** NoSQL database optimized for high-throughput, append-only time-series data

### Transformation & Orchestration
- **Apache Airflow**
- **dbt (dbt-postgres)**

### Visualization
- **Grafana**

### Infrastructure
- **Docker Compose** (local orchestration with mounted volumes)

---

### High-Level Data Flow

```text
OpenSky API → Kafka → Spark Streaming ┬→ PostgreSQL (Raw Data) ──→ dbt (Airflow DAGs) ──→ Grafana
                                      └→ Cassandra (Time-Series)
```

---

## 📂 Repository Structure

```text
.
├── docker-compose.yml
├── dags/
│   └── flight_analytics_dag.py
├── cassandra/
│   └── init.cql
├── postgres/
│   └── init.sql
├── producers/
│   ├── requirements.txt
│   └── opensky_ingestion.py
├── spark-jobs/
├── dbt_project/
│   └── flight_analytics/
│       ├── models/
│       ├── dbt_project.yml
│       └── profiles.yml
└── schemas/
```

---

##  Getting Started

### 1. Prepare the Environment

Ensure Docker and Docker Compose are installed.

Create required directories and set permissions to avoid volume mounting issues:

```bash
mkdir -p dags logs plugins dbt_project
chmod -R 777 dags logs plugins dbt_project
```

---

### 2. Start the Infrastructure

Launch the full stack:

```bash
docker compose up -d
```

> **Note:** Airflow services may take 3–5 minutes to fully initialize due to dependency installation.

---

### 3. Initialize Databases

Initialize the Cassandra schema:

```bash
docker exec -i flight-tracking-pipeline-cassandra-1 cqlsh < cassandra/init.cql
```

---

### 4. Configure Kafka Topics

Create the ingestion topic:

```bash
docker exec -it flight-tracking-pipeline-kafka-1 kafka-topics \
  --create \
  --topic flight-states \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

---

### 5. Run the Ingestion Producer

Set up a Python environment and start streaming data:

```bash
python -m venv venv
source venv/bin/activate      # Windows: .\venv\Scripts\activate
pip install -r producers/requirements.txt

python producers/opensky_ingestion.py
```

---

##  Accessing the Interfaces

| Service            | URL                     | Port Mapping     |
|--------------------|-------------------------|------------------|
| Apache Airflow     | http://localhost:8081   | 8081 → 8080      |
| Spark Master UI    | http://localhost:8080   | 8080 → 8080      |
| Grafana            | http://localhost:3001   | 3001 → 3000      |
| PostgreSQL         | localhost               | 5433 → 5432      |

---

##  Data Modeling Strategy

The pipeline uses a dual-database strategy to optimize for different access patterns.

### 1. PostgreSQL (Managed by dbt)

- **Raw Layer:** Ingested flight states (positions, velocities)
- **Dimensions:**  
  - `dim_aircraft`  
  - `dim_airlines`
- **Data Marts:** Aggregated KPIs such as:
  - Daily flight activity
  - Active flight counts
  - Airport congestion metrics

### 2. Apache Cassandra

- **Purpose:** Time-series storage for historical tracking
- **Table:** `flight_states`
- **Design:** Partitioned by `icao24` for high-throughput reads and writes

---

##  Troubleshooting

### Windows / Git Bash Path Conversion Issues

Git Bash may incorrectly translate Linux paths into Windows paths, causing execution errors.

**Solution:**
```bash
MSYS_NO_PATHCONV=1 docker exec -it <container_name> bash -c "..."
```

---

### Docker Volume Synchronization (WSL2)

If Airflow cannot detect updated files:

```bash
docker compose down
docker compose up -d --force-recreate
```

---

##  Future Improvements

- Implement anomaly detection using Spark MLlib
- Introduce dbt incremental models for efficient transformations
- Deploy infrastructure to cloud environments (Terraform)
- Add CI/CD pipelines for automated testing and deployment

---