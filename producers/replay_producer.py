import os
import time
import json
import pandas as pd
from confluent_kafka import Producer

# FIX: Read bootstrap servers from env var so this works both locally
# (localhost:9092) and inside Docker (kafka:29092)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# FIX: Read parquet path from env var so it works in Docker container too
PARQUET_PATH = os.environ.get('PARQUET_PATH', 'data/sample_flights.parquet')


class HistoricalReplayProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'flight-replay-producer'
        }
        self.producer = Producer(conf)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")

    def replay_data(self, parquet_filepath, speed_multiplier=1.0):
        print(f"Loading historical data from {parquet_filepath}...")
        try:
            df = pd.read_parquet(parquet_filepath)

            if 'time_position' not in df.columns or 'icao24' not in df.columns:
                print("Error: Parquet file missing required columns.")
                return

            df = df.sort_values(by='time_position')
            print(f"Loaded {len(df)} records. Starting replay at {speed_multiplier}x speed...")

            previous_time = None
            count = 0

            for index, row in df.iterrows():
                current_time = row['time_position']

                if previous_time is not None and current_time > previous_time:
                    time_diff = current_time - previous_time
                    sleep_time = time_diff / speed_multiplier
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                record = row.dropna().to_dict()

                self.producer.produce(
                    topic=self.topic,
                    key=str(record.get('icao24', 'UNKNOWN')),
                    value=json.dumps(record),
                    callback=self.delivery_report
                )

                previous_time = current_time
                count += 1

                if count % 5000 == 0:
                    self.producer.flush()
                    print(f"Progress: Replayed {count} messages...")

            self.producer.flush()
            print(f"Replay complete! {count} historical records pushed to Kafka.")

        except Exception as e:
            print(f"Failed during replay: {e}")


if __name__ == "__main__":
    replayer = HistoricalReplayProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic='flight-states'
    )
    replayer.replay_data(parquet_filepath=PARQUET_PATH, speed_multiplier=1000.0)
