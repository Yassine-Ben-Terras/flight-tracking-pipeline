import json
import time
from confluent_kafka import Producer
from opensky_client import OpenSkyClient

class FlightDataProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        # Kafka Producer configuration
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'flight-producer-1'
        }
        self.producer = Producer(conf)
        self.api_client = OpenSkyClient()

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""

        if err is not None:
            print(f"Message delivery failed for record {msg.key()}: {err}") 
            # this is where we would route to a Dead Letter Queue (DLQ)

    def start_polling(self, interval=10):
        print(f"Starting to poll OpenSky API every {interval} seconds...")
        try:
            while True:
                states = self.api_client.get_live_states()
                
                if states:
                    count = 0
                    for state in states:
                        # OpenSky returns an array of arrays. We map it by index to our schema structure.
                        icao24 = str(state[0]) if state[0] else "UNKNOWN"
                        
                        # Constructing the JSON message based on our Avro schema draft
                        flight_record = {
                            "icao24": icao24,
                            "time_position": state[3],
                            "callsign": state[1].strip() if state[1] else None,
                            "origin_country": state[2],
                            "longitude": state[5],
                            "latitude": state[6],
                            "baro_altitude": state[7],
                            "velocity": state[9],
                            "true_track": state[10],
                            "on_ground": state[8]
                        }

                        # Convert dictionary to JSON string
                        record_value = json.dumps(flight_record)
                        
                        # Produce message to Kafka, partitioned by icao24
                        self.producer.produce(
                            topic=self.topic,
                            key=icao24,
                            value=record_value,
                            callback=self.delivery_report
                        )
                        count += 1
                    
                    # Wait for any outstanding messages to be delivered
                    self.producer.flush()
                    print(f"Successfully pushed {count} flight records to Kafka topic '{self.topic}'.")
                
                print(f"Sleeping for {interval} seconds...\n")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nStopping producer gracefully...")
        finally:
            # Ensure everything is sent before shutting down
            self.producer.flush()

if __name__ == "__main__":
    flight_producer = FlightDataProducer(bootstrap_servers='localhost:9092', topic='flight-states')
    flight_producer.start_polling(interval=10)