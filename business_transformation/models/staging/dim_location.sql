{{ config(
    materialized='view'
) }}

select
    -- identifiers
    {{ dbt.safe_cast("Restaurant_latitude", api.Column.translate_type("string")) }} as restaurant_latitude,
    {{ dbt.safe_cast("Restaurant_longitude", api.Column.translate_type("string")) }} as restaurant_longitude,
    {{ dbt.safe_cast("Delivery_latitude", api.Column.translate_type("string")) }} as delivery_latitude,
    {{ dbt.safe_cast("Delivery_longitude", api.Column.translate_type("string")) }} as delivery_longitude,
    
    -- attributes
    {{ dbt.safe_cast("City", api.Column.translate_type("string")) }} as city,
    {{ dbt.safe_cast("order_customer_id", api.Column.translate_type("integer")) }} as order_customer_id,
    {{ dbt.safe_cast("customer_latitude", api.Column.translate_type("string")) }} as customer_latitude,
    {{ dbt.safe_cast("customer_longitude", api.Column.translate_type("string")) }} as customer_longitude,
    {{ dbt.safe_cast("Pin_code", api.Column.translate_type("string")) }} as pin_code,
    {{ dbt.safe_cast("customer_id", api.Column.translate_type("integer")) }} as customer_id

from {{ source('staging', 'dim_location') }}
