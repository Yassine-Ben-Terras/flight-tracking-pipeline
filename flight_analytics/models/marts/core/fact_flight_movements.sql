with staging as (
    select * from {{ ref('stg_flight_states') }}
),

movements as (
    select
        aircraft_id,
        event_time,
        lon,
        lat,
        altitude_meters,
        velocity_mps,
        true_track_degrees,
        is_on_ground
    from staging
    where event_time is not null
)

select * from movements