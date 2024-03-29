{{ config(
    materialized='view'
) }}

select
    -- identifiers
    {{ dbt.safe_cast("order_id", api.Column.translate_type("string")) }} as order_id,
    {{ dbt.safe_cast("customer_id", api.Column.translate_type("integer")) }} as customer_id,
    {{ dbt.safe_cast("Delivery_person_ID", api.Column.translate_type("string")) }} as delivery_person_id,
    
    -- attributes
    {{ dbt.safe_cast("Order_Date", api.Column.translate_type("string")) }} as order_date,
    {{ dbt.safe_cast("Time_taken", api.Column.translate_type("string")) }} as time_taken,
    {{ dbt.safe_cast("multiple_deliveries", api.Column.translate_type("string")) }} as multiple_deliveries,
    {{ dbt.safe_cast("Type_of_order", api.Column.translate_type("string")) }} as type_of_order,
    {{ dbt.safe_cast("order_amount", api.Column.translate_type("string")) }} as order_amount

from {{ source('staging', 'fact_order') }}
