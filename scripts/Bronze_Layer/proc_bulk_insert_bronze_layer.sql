/*
=====================================================================================
Stored Procedure: Loads Data into tables of bronze layer.
=====================================================================================
Purpose:
	This stored procedure truncates existing data from the tables in bronze layer 
	and then bulk inserts data from CSV Files.

Parameters:
	This stored Procedure accepts no parameters nor it returns any values.
*/

CREATE OR ALTER PROCEDURE bronze.load_data AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '----------------------------------------------------------------------';
		PRINT 'Loading tables in Bronze Layer';
		PRINT '----------------------------------------------------------------------';

		
		PRINT '----------------------------------------------------------------------';
		PRINT 'Loading tables of CRM';
		PRINT '----------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'Inserting Data: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DataWarehouse Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Inserting Data: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DataWarehouse Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Inserting Data: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DataWarehouse Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		PRINT '========================================================';
		PRINT 'Data Loading Completed for all CRM Tables..';
		PRINT '========================================================';

		PRINT '----------------------------------------------------------------------';
		PRINT 'Loading tables of ERP';
		PRINT '----------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'Inserting Data: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DataWarehouse Project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'Inserting Data: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DataWarehouse Project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'Inserting Data: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DataWarehouse Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '========================================================';
		PRINT 'Data Loading Completed for all ERP Tables..';
		PRINT '========================================================';

		SET @batch_end_time = GETDATE()
		PRINT '----------------------------------------------------------------------';
		PRINT 'Data Loading in Bronze Layer Completed.';
		PRINT 'Total Batch Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------------------------------';

	END TRY

	BEGIN CATCH
	PRINT '----------------------------------------------------------------------';
	PRINT 'Error Occured while Loading Data.';
	PRINT 'Error Message: '+ ERROR_MESSAGE();
	PRINT 'Error Number: '+ CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '----------------------------------------------------------------------';

	END CATCH
END

EXEC bronze.load_data;

