CREATE KEYSPACE IF NOT EXISTS flight_tracking 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_tracking;

CREATE TABLE IF NOT EXISTS flight_states (
    icao24 text,
    time_position timestamp,
    callsign text,
    origin_country text,
    longitude float,
    latitude float,
    baro_altitude float,
    velocity float,
    true_track float,
    on_ground boolean,
    PRIMARY KEY ((icao24), time_position)
) WITH CLUSTERING ORDER BY (time_position DESC);


CREATE TABLE IF NOT EXISTS airport_traffic (
    airport_code text,
    traffic_hour timestamp,
    event_time timestamp,
    icao24 text,
    event_type text,
    PRIMARY KEY ((airport_code, traffic_hour), event_time, icao24)
) WITH CLUSTERING ORDER BY (event_time DESC);


CREATE TABLE IF NOT EXISTS flight_routes (
    route_id text,
    flight_date date,
    icao24 text,
    callsign text,
    departure_time timestamp,
    arrival_time timestamp,
    flight_duration_mins int,
    PRIMARY KEY ((route_id), flight_date, icao24)
) WITH CLUSTERING ORDER BY (flight_date DESC);


CREATE TABLE IF NOT EXISTS country_traffic_density (
    origin_country text,
    window_start timestamp,
    window_end timestamp,
    active_flights bigint,
    PRIMARY KEY ((origin_country), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);


CREATE TABLE IF NOT EXISTS flight_anomalies (
    icao24 text,
    time_position timestamp,
    anomaly_type text,
    velocity double,
    baro_altitude double,
    PRIMARY KEY ((icao24), time_position)
) WITH CLUSTERING ORDER BY (time_position DESC);
