{{ config(
    materialized='table'
) }}

SELECT 
          c.customer_id,
          COUNT(DISTINCT o.order_id) AS Total_Orders,
          COUNT(DISTINCT EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) * 100 + EXTRACT(WEEK FROM PARSE_DATE('%d-%m-%Y', o.Order_Date))) AS Total_Weeks_Active,
          ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) * 100 + EXTRACT(WEEK FROM PARSE_DATE('%d-%m-%Y', o.Order_Date))), 2) AS Purchase_Frequency_Weekly
      FROM {{ ref('fact_order') }} o
      JOIN {{ ref('dim_customer') }} c ON c.customer_id = o.customer_id
      GROUP BY c.customer_id
      ORDER BY c.customer_id
