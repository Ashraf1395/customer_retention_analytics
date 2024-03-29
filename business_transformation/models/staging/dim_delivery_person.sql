{{ config(
    materialized='view'
) }}

select
    -- identifiers
    {{ dbt.safe_cast("Delivery_person_ID", api.Column.translate_type("string")) }} as delivery_person_id,
    
    -- attributes
    {{ dbt.safe_cast("Delivery_person_Age", api.Column.translate_type("string")) }} as delivery_person_age,
    {{ dbt.safe_cast("Delivery_person_Ratings", api.Column.translate_type("string")) }} as delivery_person_ratings,
    {{ dbt.safe_cast("Vehicle_condition", api.Column.translate_type("string")) }} as vehicle_condition,
    {{ dbt.safe_cast("Type_of_vehicle", api.Column.translate_type("string")) }} as type_of_vehicle

from {{ source('staging', 'dim_delivery_person') }}
