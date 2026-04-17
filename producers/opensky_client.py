import requests
import json


class OpenSkyClient:
    def __init__(self):
        self.base_url = "https://opensky-network.org/api"

    def get_live_states(self):
        """Fetches live state vectors for all currently tracked aircraft."""
        url = f"{self.base_url}/states/all"
        print(f"Fetching live flight data from {url}...")
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # The API returns a lot of data; let's just see how many flights are active right now
            states = data.get('states', [])
            print(f"Successfully retrieved data for {len(states)} currently active flights.")
            return states
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from OpenSky: {e}")
            return None

if __name__ == "__main__":
    client = OpenSkyClient()
    flight_data = client.get_live_states()
    
    # Print the very first flight record to see what the raw data looks like
    if flight_data and len(flight_data) > 0:
        print("\nSample Flight Record:")
        print(json.dumps(flight_data[0], indent=2))
        