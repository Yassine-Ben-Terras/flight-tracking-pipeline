import time
import json
import pandas as pd
from confluent_kafka import Producer

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
            # Load and sort the data chronologically
            df = pd.read_parquet(parquet_filepath)
            
            # Ensure we have the required columns before proceeding
            if 'time_position' not in df.columns or 'icao24' not in df.columns:
                print("Error: Parquet file missing required columns.")
                return

            df = df.sort_values(by='time_position')
            
            print(f"Loaded {len(df)} records. Starting replay at {speed_multiplier}x speed...")
            
            previous_time = None
            count = 0

            for index, row in df.iterrows():
                current_time = row['time_position']
                
                # Calculate how long to wait before sending the next message to simulate real-time
                if previous_time is not None and current_time > previous_time:
                    time_diff = current_time - previous_time
                    # Apply our speed multiplier (e.g., 10x speed means sleeping 1/10th the time)
                    sleep_time = time_diff / speed_multiplier
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                
                # Clean the row to standard Python types before JSON serialization
                record = row.dropna().to_dict()
                
                # Produce to Kafka
                self.producer.produce(
                    topic=self.topic,
                    key=str(record.get('icao24', 'UNKNOWN')),
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                
                previous_time = current_time
                count += 1
                
                # Handling backpressure: force a flush every 5000 messages
                if count % 5000 == 0:
                    self.producer.flush()
                    print(f"Progress: Replayed {count} messages...")

            self.producer.flush()
            print(f"Replay complete! {count} historical records pushed to Kafka.")

        except Exception as e:
            print(f"Failed during replay: {e}")

if __name__ == "__main__":
    replayer = HistoricalReplayProducer(bootstrap_servers='127.0.0.1:9092', topic='flight-states')
    replayer.replay_data(parquet_filepath='data/sample_flights.parquet', speed_multiplier=10.0)