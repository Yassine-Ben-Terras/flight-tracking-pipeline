with source as (
    select * from {{ source('raw_flights', 'flight_states') }}
),

renamed_and_casted as (
    select
        cast(icao24 as varchar) as aircraft_id,
        cast(time_position as timestamp) as event_time,
        cast(callsign as varchar) as flight_callsign,
        cast(origin_country as varchar) as country_of_origin,
        cast(longitude as double precision) as lon,
        cast(latitude as double precision) as lat,
        cast(baro_altitude as double precision) as altitude_meters,
        cast(velocity as double precision) as velocity_mps,
        cast(true_track as double precision) as true_track_degrees,
        cast(on_ground as boolean) as is_on_ground,
        
        -- Applying the new Macros
        {{ meters_to_feet('baro_altitude') }} as altitude_feet,
        {{ mps_to_knots('velocity') }} as velocity_knots

    from source
    where time_position is not null
)

select * from renamed_and_casted