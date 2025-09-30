/*
-------------------------------------------------------
Cumulative Sales Analysis
-------------------------------------------------------
*/

-- Cumulative sales and moving average price over year
SELECT 
	order_year,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_year) AS cumulative_sales,
	AVG(avg_price) OVER(ORDER BY order_year) AS moving_average_price
FROM
(
	SELECT 
		DATETRUNC(year, sales_order_date) AS order_year,
		SUM(total_sales) AS total_sales,
		AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE sales_order_date IS NOT NULL
	GROUP BY DATETRUNC(year, sales_order_date)
)t

-- Cumulative sales over time based on month 
SELECT 
	order_date,
	Total_Sales,
	SUM(Total_Sales) OVER(PARTITION BY order_date ORDER BY order_date) AS cumulative_monthly_sales
FROM
(
	SELECT 
		DATETRUNC(month,sales_order_date) AS order_date,
		SUM(total_sales) AS Total_Sales
	FROM gold.fact_sales
	WHERE sales_order_date IS NOT NULL
	GROUP BY DATETRUNC(month,sales_order_date) 
)t

/*
-------------------------------------------------------
Performance Analysis
-------------------------------------------------------
*/

-- Analyze yearly performance of products by comparing their sales to both average sales
-- performance of product and previous's year sales
WITH cte AS
(
SELECT
YEAR(s.sales_order_date) AS year,
p.product_name AS product_name,
SUM(s.total_sales) AS total_sales
FROM gold.fact_sales s
INNER JOIN gold.dim_product_info p
ON s.product_key = p.product_key
WHERE YEAR(sales_order_date) IS NOT NULL
GROUP BY YEAR(s.sales_order_date), p.product_name
)
SELECT year,
product_name,
total_sales AS current_sales,
AVG(total_sales) OVER (PARTITION BY product_name) AS avg_sales,
total_sales - AVG(total_sales) OVER (PARTITION BY product_name) AS sales_diff,
CASE WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) > 0 THEN 'Higher'
	 WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) < 0 THEN 'Lower'
	 ELSE 'Same'
END AS sales_flag,
LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS previous_year_sales,
total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS diff_py,
CASE WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) > 0 THEN 'Increase'
	 WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) < 0 THEN 'Decrease'
	 ELSE 'Same'
END AS year_to_year_flag
FROM cte
ORDER BY product_name,year;

/*
-------------------------------------------------------
Part to whole Analysis
-------------------------------------------------------
*/
with total_sales_category AS
(
SELECT 
category,
SUM(total_sales) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_product_info p
ON p.product_key = s.product_key
GROUP BY category
)
SELECT 
category,
total_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100,2),' %')  AS part_to_whole
FROM total_sales_category;

/*
-------------------------------------------------------
Measure - Products based on Cost Range
-------------------------------------------------------
*/
WITH measure_cost_range AS
(
SELECT product_key, product_id, product_name, cost, 
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500 -1000'
	 WHEN cost BETWEEN 1000 AND 1500 THEN '1000 - 1500'
	 ELSE 'Above 1500'
END AS cost_range
FROM gold.dim_product_info
)
SELECT COUNT(product_key) AS total_products,
cost_range
FROM 
measure_cost_range
GROUP BY cost_range
ORDER BY total_products

/*
-------------------------------------------------------
Measure - Grouping based on spending and lifespan by customer
Prime - Purchase Amount > 5000 and more than 12 months old
Regular - Purchase Amount < 5000 and more than 12 months old
New -  Less than 12 months old
-------------------------------------------------------
*/
WITH measure_customer_spending AS
(
SELECT c.customer_key AS customer_key, 
sum(s.total_sales) AS spending,
DATEDIFF(MONTH, MIN(s.sales_order_date), MAX(s.sales_order_date)) AS months
FROM gold.fact_sales s 
LEFT JOIN gold.dim_customer_info c
ON s.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT 
customer_segment,
COUNT(customer_key) AS customer_count
FROM
(
SELECT 
customer_key,
CASE WHEN months >= 12 AND spending > 5000 THEN 'Prime'
	 WHEN months >= 12 AND spending < 5000 THEN 'Regular'
	 ELSE 'New'
END AS customer_segment
FROM measure_customer_spending
)t
GROUP BY 
customer_segment;