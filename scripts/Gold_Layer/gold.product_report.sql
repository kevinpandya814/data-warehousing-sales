/*
===============================================================================
View: Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
*/

--SELECT * FROM gold.fact_sales f LEFT JOIN gold.dim_product_info d ON f.product_key = d.product_key
WITH base_query AS
(
	SELECT
		f.sales_order_number,
		f.product_key AS product_key,
		f.customer_key,
		f.quantity,
		f.total_sales,
		f.sales_order_date,
		d.product_name,
		d.category,
		d.subcategory,
		d.cost
	FROM gold.fact_sales f 
	LEFT JOIN gold.dim_product_info d
	ON f.product_key = d.product_key
	WHERE sales_order_date IS NOT NULL
),
 product_aggregation AS
(
	SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	COUNT(sales_order_number) AS total_orders,
	SUM(total_sales) AS total_sales,
	SUM(quantity) AS total_quantitiy,
	COUNT(DISTINCT(customer_key)) AS total_customers,
	DATEDIFF(MONTH, MIN(sales_order_date), MAX(sales_order_date)) AS lifespan,
	MAX(sales_order_date) AS last_sale_date,
	ROUND(AVG(CAST(total_sales AS FLOAT)/ NULLIF(quantity,0)),2) AS avg_selling_price
	FROM base_query
	GROUP BY
	product_key,
	product_name,
	category,
	subcategory,
	cost
)
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE WHEN total_sales > 40000 THEN 'High Performer'
		 WHEN total_sales BETWEEN 10000 AND 40000 THEN 'Avg Performer'
		 ELSE 'Low performer'
	END AS product_segment,
	total_orders,
	total_sales,
	total_quantitiy,
	lifespan,
	avg_selling_price,
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales / total_orders
	END AS average_selling_value,
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales / lifespan
	END AS average_monthly_revenue
FROM product_aggregation