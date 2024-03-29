{{ config(
    materialized='table'
) }}


SELECT 
          c.customer_id,
          SUM(fo.order_amount) AS Total_Revenue,
          ROUND(AVG(fo.order_amount),2) AS Avg_Purchase_Rate
      FROM {{ ref('dim_customer') }} c
      JOIN {{ ref('fact_order') }} fo ON fo.customer_id = c.customer_id
      GROUP BY c.customer_id
      ORDER BY Total_Revenue DESC
