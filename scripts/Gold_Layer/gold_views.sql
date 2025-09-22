/*
=====================================================================================
DDL: Views for Gold layer
=====================================================================================
Purpose:
	This script creates views for the gold layer of our architecture following the fact and dimension 
	tables of a star schema.

	Each view combines schemas from the silver layer into fact and dimension tables.
*/

/*
=====================================================================================
Create View for Customer info
=====================================================================================
*/
IF OBJECT_ID('gold.dim_customer_info', 'V') IS NOT NULL
	DROP VIEW gold.dim_customer_info;
GO
CREATE VIEW gold.dim_customer_info AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
		cst_id AS customer_id,
		cst_key AS customer_number,
		cst_firstname AS first_name,
		cst_lastname AS last_name,
		cntry AS country,
		CASE WHEN cst_gndr != 'Unknown' THEN cst_gndr
			 ELSE COALESCE(gen, 'Unknown')
		END AS gender,
		cst_marital_status AS marital_status,
		bdate AS birth_date,
		cst_create_date AS create_date
	FROM silver.crm_cust_info cci
	LEFT JOIN silver.erp_cust_az12 eci
	ON cci.cst_key = eci.cid
	LEFT JOIN silver.erp_loc_a101 loc_info
	ON cci.cst_key = loc_info.cid;
GO

/*
=====================================================================================
Create View for Product info
=====================================================================================
*/
IF OBJECT_ID('gold.dim_product_info', 'V') IS NOT NULL
	DROP VIEW gold.dim_product_info;
GO
CREATE VIEW gold.dim_product_info AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key,
		prd_id AS product_id,
		prd_sl_key AS product_number,
		prd_nm AS product_name,
		prd_cat_id AS product_category_id,
		cat AS category,
		subcat AS subcategory,
		maintenance AS maintainence,
		prd_cost AS cost, 
		prd_start_dt AS start_date
	FROM silver.crm_prd_info
	LEFT JOIN silver.erp_px_cat_g1v2
	ON prd_cat_id = id
	WHERE prd_end_dt IS NULL;
GO

/*
=====================================================================================
Create View for Sales
=====================================================================================
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
	SELECT 
		sls_ord_num AS sales_order_number,
		product_key AS product_key,
		customer_id AS customer_id,
		sls_order_dt AS sales_order_date,
		sls_ship_dt AS sales_ship_date,
		sls_due_dt AS sales_due_date,
		sls_price AS price,
		sls_quantity AS quantity,
		sls_sales AS total_sales
	FROM silver.crm_sales_details
	LEFT JOIN gold.dim_product_info ON sls_prd_key = product_number
	LEFT JOIN gold.dim_customer_info ON sls_cust_id = customer_id
GO 
