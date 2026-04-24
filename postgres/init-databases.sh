#!/bin/bash
set -e

# This script runs automatically when the PostgreSQL container starts for the first time.
# It creates the separate Airflow metadata database and the flight_states table
# in the data warehouse so dbt has a source table to work with.

# --- Create the Airflow metadata database ---
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow_metadata;
    GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO $POSTGRES_USER;
EOSQL

echo "✅ Created airflow_metadata database."

# --- Create the flight_states table in the data warehouse ---
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS flight_states (
        icao24          VARCHAR(10),
        time_position   TIMESTAMP,
        callsign        VARCHAR(20),
        origin_country  VARCHAR(100),
        longitude       DOUBLE PRECISION,
        latitude        DOUBLE PRECISION,
        baro_altitude   DOUBLE PRECISION,
        velocity        DOUBLE PRECISION,
        true_track      DOUBLE PRECISION,
        on_ground       BOOLEAN
    );
EOSQL

echo "✅ Created flight_states table in flight_warehouse."
