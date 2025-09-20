/*
=====================================================================================
Stored Procedure: Loads Data into tables of silver layer from bronze layer.
=====================================================================================
Purpose:
	This stored procedure performs data cleaning, normalization and derive columns from existing data of bronze layer.

Parameters:
	This stored Procedure accepts no parameters nor it returns any values.
*/

CREATE OR ALTER PROCEDURE silver.load_data AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '--------------------------------------------------------------------------------';
		PRINT 'Loading tables into Silver Layer.';
		PRINT '--------------------------------------------------------------------------------';

		PRINT '--------------------------------------------------------------------------------';
		PRINT 'Loading tables from CRM';
		PRINT '--------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting data: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN cst_marital_status = 'M' THEN 'Married'
			 WHEN cst_marital_status = 'S' THEN 'Single'
			 ELSE 'Unknown'
		END AS cst_marital_status, 
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown'
		END AS cst_gndr,
		cst_create_date
		FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
		FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)t 
		WHERE flag = 1 ;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		-------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting data: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (
		prd_id, 
		prd_key, 
		prd_cat_id, 
		prd_sl_key, 
		prd_nm, 
		prd_cost, 
		prd_line,
		prd_start_dt,
		prd_end_dt)

		SELECT prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS prd_cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_sl_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Sport'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'Unknown'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt ,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';
		------------------------------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting data: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) < 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity THEN ABS(sls_price) * sls_quantity
			 ELSE ABS(sls_sales)
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';
		---------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting data: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate 
		END AS bdate,
		CASE
			 WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			 WHEN UPPER(TRIM(gen)) IS NULL THEN 'Unknown'
			 ELSE gen
		END AS gen

		FROM 
		bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';
		-----------------------------------------------------------------------------

		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting data: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry)
		SELECT 
		REPLACE(cid,'-',''),
		CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'United States') THEN 'United States of America'
			 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IS NULL THEN 'Unknown'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';
		-------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting data: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		SELECT id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		SET @batch_end_time = GETDATE()
		PRINT '----------------------------------------------------------------------';
		PRINT 'Data Loading in Silver Layer Completed.';
		PRINT 'Total Batch Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------------------------------';
	END TRY

	BEGIN CATCH
		PRINT '--------------------------------------------------------------------------------';
		PRINT 'Error Occured while Loading Data.';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------------------------------------------';
	END CATCH

END

EXEC silver.load_data;