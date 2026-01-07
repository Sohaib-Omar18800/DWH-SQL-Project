/*
**********************************************************
Store Procedure: Load Silver Layer (Bronze --> Silver)
**********************************************************
Script Purpose:
	This stored procedure performs ETL process to populate the 'Silver' schema from 'Bronze' Tables.
	It performs the following actions:
		- Truncate the silver tables before loading data.
		- Uses 'INSERT SELECT' method to load cleaned and transformed data from bronze tables into silver tables.
Parameters:
	None.
	This stored procedure dont accept any parameters or return any values.
Usage Example:
	EXEC silver.load_silver;
************************************************************
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME
		PRINT('***********************');
		PRINT('LOADING SILVER LAYER');
		PRINT('***********************');

		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		PRINT('LOADING crm TABLES');
		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');

		PRINT('**********************************')
		PRINT('TRUNCATING silver.crm_prd_info')
		PRINT('**********************************')
		SET @batch_start_time = GETDATE()
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_prd_info
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.crm_prd_info')
		PRINT('**********************************')
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		select 
		prd_id,
		REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_')as cat_id,
		SUBSTRING(TRIM(prd_key),7,len(TRIM(prd_key))) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'Unidentified' 
		END AS prd_line,
		prd_start_dt,
		lead(dateadd(day,-1,prd_start_dt)) over(partition by prd_key order by prd_start_dt) as prd_end_dt
		from 
		bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')
		PRINT('------------------------------------------------------------------------------------------------------------')
	--==========================================================================================================
		PRINT('**********************************')
		PRINT('TRUNCATING silver.crm_cust_info')
		PRINT('**********************************')
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_cust_info
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.crm_cust_info')
		PRINT('**********************************')
		INSERT INTO 
		silver.crm_cust_info
		(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		select 
		cst_id,
		TRIM(cst_key) as cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'Unidentified'
		END as cst_material_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'Unidentified'
		END as cst_gndr,
		cst_create_date
		from 
		(SELECT cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date,
		ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc ) as flag_last
		FROM bronze.crm_cust_info) as d
		where flag_last = 1 and cst_id is not null and cst_create_date > '1980-01-01'
		SET @end_time = GETDATE()
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')
		PRINT('------------------------------------------------------------------------------------------------------------')
	--==========================================================================================================
		PRINT('**********************************')
		PRINT('TRUNCATING silver.crm_sale_details')
		PRINT('**********************************')
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_sale_details
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.crm_sale_details')
		PRINT('**********************************')
		INSERT INTO silver.crm_sale_details(
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
		select 
		TRIM(sls_ord_num) AS sls_ord_num,
		TRIM(sls_prd_key) AS sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt IS NULL OR LEN(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt IS NULL OR LEN(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt IS NULL OR LEN(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END AS sls_due_dt,
		CASE
			WHEN 
			(sls_sales IS NULL OR sls_sales != ABS(sls_quantity) * ABS(sls_price) OR sls_sales = 0)
			AND (sls_price IS NOT NULL OR sls_price != 0)
			THEN ABS(sls_price) * ABS(sls_quantity)
			ELSE ISNULL(ABS(sls_sales),0)
		END AS sls_sales,
		ABS(sls_quantity) AS sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price = 0 THEN ABS(sls_sales / IIF(sls_quantity=0,1,sls_quantity))
			ELSE ABS(sls_price)
		END AS sls_price

		FROM bronze.crm_sale_details
		SET @end_time = GETDATE()
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')
		PRINT('------------------------------------------------------------------------------------------------------------')
	--==========================================================================================================
		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');
		PRINT('LOADING crm TABLES');
		PRINT('/\/\/\/\/\/\/\/\/\/\/\/\');

		PRINT('**********************************')
		PRINT('TRUNCATING silver.erp_cust_az12')
		PRINT('**********************************')
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.erp_cust_az12')
		PRINT('**********************************')
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)
		SELECT 
		CASE
			WHEN cid LIKE 'NAS%'
			THEN SUBSTRING(cid,4,LEN(TRIM(cid)))
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'Unidentified'
		END as gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')
		PRINT('------------------------------------------------------------------------------------------------------------')
	--==========================================================================================================
		PRINT('**********************************')
		PRINT('TRUNCATING silver.erp_loc_a101')
		PRINT('**********************************')
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.erp_loc_a101')
		PRINT('**********************************')
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry)
		SELECT 
		REPLACE(TRIM(cid),'-','') AS cid,
		CASE
			WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'Unidentified'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS')
		PRINT('------------------------------------------------------------------------------------------------------------')
	--==========================================================================================================
		PRINT('**********************************')
		PRINT('TRUNCATING silver.erp_px_cat_g1v2')
		PRINT('**********************************')
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT('**********************************')
		PRINT('INSERTING DATA TO silver.erp_px_cat_g1v2')
		PRINT('**********************************')
		INSERT INTO silver.erp_px_cat_g1v2(
		id,cat,subcat,maintenance)
		SELECT 
		id,
		TRIM(cat),
		TRIM(subcat),
		TRIM(maintenance)
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT('>> LOADING DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS');
		PRINT('------------------------------------------------------------------------------------------------------------')
		PRINT('-- LOADING SILVER LAYER COMPELETED');
		PRINT('		>> FULL LOAD DURATION: ' + CAST(DATEDIFF(second,@batch_start_time,@end_time) AS NVARCHAR) + 'SECONDS');
		PRINT('------------------------------------------------------------------------------------------------------------')
	END TRY
	BEGIN CATCH
		PRINT('TRACE ERROR()')
		PRINT('^^^^^^^^^^^^^^^')
		PRINT('ERROR OCCURED DURRING LOADING SILVER LAYER');
		PRINT('ERROR MESSAGE'+ ERROR_MESSAGE());
		PRINT('ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR));
		PRINT('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
	END CATCH
END
