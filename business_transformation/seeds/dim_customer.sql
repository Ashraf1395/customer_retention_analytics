{{ config(
    materialized='view'
) }}

select
    -- identifiers
    {{ dbt.safe_cast("Age", api.Column.translate_type("string")) }} as age,
    {{ dbt.safe_cast("Gender", api.Column.translate_type("string")) }} as gender,
    {{ dbt.safe_cast("Marital_Status", api.Column.translate_type("string")) }} as marital_status,
    
    -- attributes
    {{ dbt.safe_cast("Occupation", api.Column.translate_type("string")) }} as occupation,
    {{ dbt.safe_cast("Monthly_Income", api.Column.translate_type("string")) }} as monthly_income,
    {{ dbt.safe_cast("Educational_Qualifications", api.Column.translate_type("string")) }} as educational_qualifications,
    {{ dbt.safe_cast("Family_size", api.Column.translate_type("string")) }} as family_size,
    {{ dbt.safe_cast("latitude", api.Column.translate_type("string")) }} as latitude,
    {{ dbt.safe_cast("longitude", api.Column.translate_type("string")) }} as longitude,
    {{ dbt.safe_cast("Pin_code", api.Column.translate_type("string")) }} as pin_code,
    {{ dbt.safe_cast("Medium_P1", api.Column.translate_type("string")) }} as medium_p1,
    {{ dbt.safe_cast("Medium_P2", api.Column.translate_type("string")) }} as medium_p2,
    {{ dbt.safe_cast("Meal_P1", api.Column.translate_type("string")) }} as meal_p1,
    {{ dbt.safe_cast("Meal_P2", api.Column.translate_type("string")) }} as meal_p2,
    {{ dbt.safe_cast("Perference_P1", api.Column.translate_type("string")) }} as perference_p1,
    {{ dbt.safe_cast("Perference_P2", api.Column.translate_type("string")) }} as perference_p2,
    {{ dbt.safe_cast("Ease_and_convenient", api.Column.translate_type("string")) }} as ease_and_convenient,
    {{ dbt.safe_cast("Time_saving", api.Column.translate_type("string")) }} as time_saving,
    {{ dbt.safe_cast("More_restaurant_choices", api.Column.translate_type("string")) }} as more_restaurant_choices,
    {{ dbt.safe_cast("Easy_Payment_option", api.Column.translate_type("string")) }} as easy_payment_option,
    {{ dbt.safe_cast("More_Offers_and_Discount", api.Column.translate_type("string")) }} as more_offers_and_discount,
    {{ dbt.safe_cast("Good_Food_quality", api.Column.translate_type("string")) }} as good_food_quality,
    {{ dbt.safe_cast("Good_Tracking_system", api.Column.translate_type("string")) }} as good_tracking_system,
    {{ dbt.safe_cast("Self_Cooking", api.Column.translate_type("string")) }} as self_cooking,
    {{ dbt.safe_cast("Health_Concern", api.Column.translate_type("string")) }} as health_concern,
    {{ dbt.safe_cast("Late_Delivery", api.Column.translate_type("string")) }} as late_delivery,
    {{ dbt.safe_cast("Poor_Hygiene", api.Column.translate_type("string")) }} as poor_hygiene,
    {{ dbt.safe_cast("Bad_past_experience", api.Column.translate_type("string")) }} as bad_past_experience,
    {{ dbt.safe_cast("Unavailability", api.Column.translate_type("string")) }} as unavailability,
    {{ dbt.safe_cast("Unaffordable", api.Column.translate_type("string")) }} as unaffordable,
    {{ dbt.safe_cast("Long_delivery_time", api.Column.translate_type("string")) }} as long_delivery_time,
    {{ dbt.safe_cast("Delay_of_delivery_person_getting_assigned", api.Column.translate_type("string")) }} as delay_of_delivery_person_getting_assigned,
    {{ dbt.safe_cast("Delay_of_delivery_person_picking_up_food", api.Column.translate_type("string")) }} as delay_of_delivery_person_picking_up_food,
    {{ dbt.safe_cast("Wrong_order_delivered", api.Column.translate_type("string")) }} as wrong_order_delivered,
    {{ dbt.safe_cast("Missing_item", api.Column.translate_type("string")) }} as missing_item,
    {{ dbt.safe_cast("Order_placed_by_mistake", api.Column.translate_type("string")) }} as order_placed_by_mistake,
    {{ dbt.safe_cast("Influence_of_time", api.Column.translate_type("string")) }} as influence_of_time,
    {{ dbt.safe_cast("Order_Time", api.Column.translate_type("string")) }} as order_time,
    {{ dbt.safe_cast("Maximum_wait_time", api.Column.translate_type("string")) }} as maximum_wait_time,
    {{ dbt.safe_cast("Residence_in_busy_location", api.Column.translate_type("string")) }} as residence_in_busy_location,
    {{ dbt.safe_cast("Google_Maps_Accuracy", api.Column.translate_type("string")) }} as google_maps_accuracy,
    {{ dbt.safe_cast("Good_Road_Condition", api.Column.translate_type("string")) }} as good_road_condition,
    {{ dbt.safe_cast("Low_quantity_low_time", api.Column.translate_type("string")) }} as low_quantity_low_time

from {{ source('staging', 'dim_customer') }}
