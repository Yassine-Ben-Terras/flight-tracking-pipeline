{{
    config(
        materialized='incremental'
    )
}}

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

    {% if is_incremental() %}
      -- This ensures we only append records newer than what is already in the table
      and event_time > (select max(event_time) from {{ this }})
    {% endif %}
)

select * from movements