FROM python:3.11-slim

WORKDIR /app

COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer source files
COPY producers/ .

# Copy the parquet data
RUN mkdir -p /app/data
COPY data/sample_flights.parquet /app/data/sample_flights.parquet

# Wait for Kafka, then replay forever with 60s pause between runs
CMD ["bash", "-c", "echo 'Waiting for Kafka...' && sleep 30 && while true; do python replay_producer.py && echo 'Replay done, sleeping 60s...' && sleep 60; done"]
