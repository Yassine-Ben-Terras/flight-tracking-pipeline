import pandas as pd
import numpy as np
import time
import os

def generate_mock_flight_data(num_records=10000, output_file='data/sample_flights.parquet'):

    print(f"Generating {num_records} mock flight records...")
    
    # Use current time as the starting point
    base_time = int(time.time()) - (num_records * 2) # Start in the past
    
    # Create an array of timestamps (spaced out to simulate a timeline)
    timestamps = [base_time + (i * 2) for i in range(num_records)]
    
    # Simulate 5 unique airplanes flying
    icao24_list = ['A12345', 'B67890', 'C11223', 'D44556', 'E77889']
    
    data = {
        'icao24': [icao24_list[i % 5] for i in range(num_records)],
        'time_position': timestamps,
        'callsign': [f"MOCK{i%5}" for i in range(num_records)],
        'origin_country': ['TestCountry'] * num_records,
        'longitude': np.random.uniform(-10.0, 10.0, num_records),
        'latitude': np.random.uniform(30.0, 45.0, num_records),
        'baro_altitude': np.random.uniform(10000.0, 35000.0, num_records),
        'velocity': np.random.uniform(200.0, 250.0, num_records),
        'true_track': np.random.uniform(0.0, 360.0, num_records),
        'on_ground': [False] * num_records
    }
    
    df = pd.DataFrame(data)
    
    # Ensure the data directory exists one level up from the producers folder
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save to Parquet format using pyarrow
    df.to_parquet(output_file, engine='pyarrow')
    print(f"Successfully saved mock data to {output_file}")

if __name__ == "__main__":
    generate_mock_flight_data(num_records=5000)