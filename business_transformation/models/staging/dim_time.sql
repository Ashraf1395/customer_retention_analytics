{{ config(
    materialized='view'
) }}

select
    -- attributes
    {{ dbt.safe_cast("Order_Date", api.Column.translate_type("string")) }} as order_date,
    {{ dbt.safe_cast("Time_Ordered", api.Column.translate_type("string")) }} as time_ordered,
    {{ dbt.safe_cast("Time_Order_picked", api.Column.translate_type("string")) }} as time_order_picked,
    {{ dbt.safe_cast("Weatherconditions", api.Column.translate_type("string")) }} as weather_conditions,
    {{ dbt.safe_cast("Road_traffic_density", api.Column.translate_type("string")) }} as road_traffic_density,
    {{ dbt.safe_cast("Festival", api.Column.translate_type("string")) }} as festival

from {{ source('staging', 'dim_time') }}
