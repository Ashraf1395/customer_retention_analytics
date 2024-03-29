{{ config(
    materialized='table'
) }}

SELECT 
          EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) AS Year,
          EXTRACT(MONTH FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) AS Month,
          EXTRACT(DAY FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) AS Day,
          COUNT(DISTINCT c.customer_id) AS Total_Customers,
          COUNT(DISTINCT CASE WHEN lo.customer_id IS NULL THEN c.customer_id ELSE NULL END) AS Churned_Customers,
          ROUND((COUNT(DISTINCT CASE WHEN lo.customer_id IS NULL THEN c.customer_id ELSE NULL END) / COUNT(DISTINCT c.customer_id)) * 100, 2) AS Churn_Rate
      FROM {{ ref('fact_order') }} o
      LEFT JOIN (
          SELECT DISTINCT customer_id, 
                          EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Next_Order_Year,
                          EXTRACT(MONTH FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Next_Order_Month,
                          EXTRACT(DAY FROM PARSE_DATE('%d-%m-%Y', Order_Date)) AS Next_Order_Day
          FROM {{ ref('fact_order') }}
      ) lo 
      ON lo.customer_id = o.customer_id 
          AND lo.Next_Order_Year = EXTRACT(YEAR FROM PARSE_DATE('%d-%m-%Y', o.Order_Date))
          AND lo.Next_Order_Month = EXTRACT(MONTH FROM PARSE_DATE('%d-%m-%Y', o.Order_Date))
          AND lo.Next_Order_Day = EXTRACT(DAY FROM PARSE_DATE('%d-%m-%Y', o.Order_Date)) + 1
      JOIN {{ ref('dim_customer') }} c ON c.customer_id = o.customer_id
      GROUP BY Year, Month, Day
      ORDER BY Year, Month, Day
