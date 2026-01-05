/*
**********************************************************
Store Procedure: Load Bronze Layer (Soure --> Bronze)
**********************************************************
Script Purpose:
	This stored procedure load data into the 'bronze' schema from external csv files.
	It performs the following actions:
		- Truncate the bronze tables before loading data.
		- Uses 'BULK INSERT' method to load data from csv files into bronze tables.
Parameters:
	None.
	This stored procedure dont accept any parameters or return any values.
Usage Example:
	EXEC bronze.load_bronze;
************************************************************
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME
	BEGIN TRY
		PRINT('***********************');
		PRINT('LOADING BRONZE LAYER');
		PRINT('***********************');

		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		PRINT('LOADING crm TABLES');
		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.crm_cust_info')
		PRINT('""""""""""""""""""""""""');
		SET @start_time = GETDATE();
		SET @batch_start_time = GETDATE();
		-- TRUNCATE AND FULL LOAD THE TABLE crm_cust_info
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.crm_cust_info')
		PRINT('""""""""""""""""""""""""');
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')

		-- TRUNCATE AND FULL LOAD THE TABLE crm_prd_info
		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.crm_prd_info')
		PRINT('""""""""""""""""""""""""');
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.crm_prd_info')
		PRINT('""""""""""""""""""""""""');
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')

		-- TRUNCATE AND FULL LOAD THE TABLE crm_sale_details
		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.crm_sale_details')
		PRINT('""""""""""""""""""""""""');
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sale_details;

		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.crm_sale_details')
		PRINT('""""""""""""""""""""""""');
		BULK INSERT bronze.crm_sale_details
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')

		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		PRINT('LOADING erp TABLES');
		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		-- TRUNCATE AND FULL LOAD THE TABLE erp_px_cat_g1v2
		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2')
		PRINT('""""""""""""""""""""""""');
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_px_cat_g1v2')
		PRINT('""""""""""""""""""""""""');

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')

		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.erp_cust_az12')
		PRINT('""""""""""""""""""""""""');
		-- TRUNCATE AND FULL LOAD THE TABLE erp_cust_az12
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_cust_az12')
		PRINT('""""""""""""""""""""""""');

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')


		PRINT('""""""""""""""""""""""""');
		PRINT('>> TRUNCATING TABLE: bronze.erp_loc_a101')
		PRINT('""""""""""""""""""""""""');
		-- TRUNCATE AND FULL LOAD THE TABLE erp_loc_a101
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT('""""""""""""""""""""""""');
		PRINT('>> INSERTING DATA INTO TABLE: bronze.erp_loc_a101')
		PRINT('""""""""""""""""""""""""');
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Udemy Courses\SQL\project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS');
		PRINT('------------------------------------------')
		PRINT('-- LOADING BRONZE LAYER COMPELETED');
		PRINT('		>> FULL LOAD DURATION: ' + CAST(DATEDIFF(second,@batch_start_time,@end_time) AS NVARCHAR) + 'SECONDS');
	END TRY
	BEGIN CATCH
		PRINT('TRACE ERROR()')
		PRINT('^^^^^^^^^^^^^^^^^^^^^^^^^')
		PRINT('ERROR OCCURED DURRING LOADING BRONZE LAYER');
		PRINT('ERROR MESSAGE'+ ERROR_MESSAGE());
		PRINT('ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR));
		PRINT('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
	END CATCH
END

EXEC bronze.load_bronze
