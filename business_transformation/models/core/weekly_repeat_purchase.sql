{{ config(
    materialized='table'
) }}

SELECT 
          order_counts.Year,order_counts.Month,order_counts.Week,
          ROUND(sum(distinct total_orders)/COUNT(DISTINCT customer_id) * 100, 2) AS Weekly_Repeat_Purchase_Rate,
          sum(distinct total_orders) as Weekly_Repeat_Orders
      FROM (
          SELECT 
              customer_id,
              EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Year,
              EXTRACT(Month FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Month,
              EXTRACT(WEEK FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Week,
              COUNT(DISTINCT order_id) AS total_orders
          FROM {{ ref('fact_order') }}
          GROUP BY customer_id, Year,Month,Week
      ) AS order_counts
      Group by Year,Month,Week
      Order by Year,Month,Week
