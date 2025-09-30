/*
=====================================================================================
View: Customer Report 
=====================================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
*/
--SELECT * FROM gold.fact_sales s LEFT JOIN gold.dim_customer_info c ON c.customer_key = s.customer_key;

CREATE VIEW gold.customer_report AS 

WITH base_query AS
(
	SELECT 
		s.sales_order_number AS order_number,
		s.product_key,
		s.sales_order_date AS order_date,
		s.quantity,
		s.total_sales,
		c.customer_key,
		customer_number,
		CONCAT (first_name, ' ', last_name) AS customer_name,
		c.country,
		c.gender,
		c.marital_status,
		DATEDIFF(year,c.birth_date,GETDATE()) AS age
	FROM gold.fact_sales s 
	LEFT JOIN gold.dim_customer_info c
	ON c.customer_key = s.customer_key
	WHERE s.sales_order_date IS NOT NULL
),
customer_aggregation AS
(
	SELECT 
		customer_key,
		customer_number,
		customer_name,
		age,
		country,
		gender,
		marital_status,
		COUNT(DISTINCT(order_number)) AS total_orders,
		SUM(total_sales) AS all_time_sales,
		SUM(quantity) AS total_quantity,
		MAX(order_date) AS last_order_date,
		COUNT(DISTINCT(product_key)) AS total_products,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age,
	country,
	gender,
	marital_status
)
SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE WHEN age < 20 THEN 'Under 20'
		 WHEN age BETWEEN 20 AND 30 THEN '20-30'
		 WHEN age BETWEEN 30 AND 40 THEN '30-40'
		 WHEN age BETWEEN 40 AND 50 THEN '40-50'
		 ELSE '60 and above'
	END AS age_group,
	CASE WHEN lifespan >= 12 AND all_time_sales > 5000 THEN 'Prime'
		 WHEN lifespan >= 12 AND all_time_sales < 5000 THEN 'Regular'
		 ELSE 'New'
	END AS customer_segment,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency_in_months,
	total_orders,
	all_time_sales,
	total_quantity,
	total_products,
	lifespan,
	CASE WHEN all_time_sales = 0 THEN 0
		 ELSE all_time_sales / total_orders
	END AS avg_order_value,
	CASE WHEN lifespan = 0 THEN all_time_sales
		 ELSE all_time_sales / lifespan
	END AS avg_monthly_spend
FROM customer_aggregation;