{{
    config(
        materialized='table'
    )
}}

with staging as (
    select * from {{ ref('stg_flight_states') }}
),

unique_aircraft as (
    select distinct
        aircraft_id,
        flight_callsign,
        country_of_origin
    from staging
    where aircraft_id is not null
)

select * from unique_aircraft