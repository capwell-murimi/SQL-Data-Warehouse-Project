/*
=======================================================================
THIS IS A STORED PROCEDURE TO LOAD DATA INTO THE SILVER LAYER
Procedure Name : silver.load_bronze
Purpose        : Load raw source data (CRM & ERP) into the Bronze layer
Layer          : Silver (Cleaning Layer)

EXECUTION:
	EXEC silver.load_silver
=======================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
	DECLARE
		@starttime DATETIME,
		@endtime DATETIME,
		@startbatchtime DATETIME,
		@endbatchtime DATETIME
	BEGIN TRY
		SET @startbatchtime = GETDATE()
		PRINT 'Loading into CRM Silver'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Inserting Data into silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_maritual_status,
			cst_gndr,
			cst_create_date
		)
			SELECT	cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE
						WHEN UPPER(TRIM(cst_maritual_status)) = 'M' THEN 'Married'
						WHEN UPPER(TRIM(cst_maritual_status)) = 'S' THEN 'Single'
						ELSE 'n/a'
					END AS cst_maritual_status,
					CASE
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						ELSE 'n/a'
					END AS cst_gndr,
					cst_create_date
				FROM (
					SELECT *,
						ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) 
						AS row_num
					FROM 
				bronze.crm_cust_info WHERE cst_id IS NOT NULL)t WHERE row_num = 1;
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(second,@starttime,@endtime)AS NVARCHAR) + ' seconds' 

		PRINT '###################################################################'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>>Inserting Data into silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT * FROM
		(SELECT prd_id,
				--prd_key,
				CAST(REPLACE(SUBSTRING(prd_key,1,5), '-','_')AS NVARCHAR) AS cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE UPPER(TRIM(prd_line)) 
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'O' THEN 'Other sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line,
				prd_start_dt,
				DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
				FROM (
			SELECT *, ROW_NUMBER() OVER(PARTITION BY prd_id ORDER BY prd_start_dt DESC)
			AS row_num 
			FROM bronze.crm_prd_info 
			WHERE prd_id IS NOT NULL)t
		WHERE row_num = 1)t2 ;
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND,@starttime,@endtime)AS NVARCHAR) + ' seconds'

		PRINT '###################################################################'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>>Inserting Data Into silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_pred_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
		)

		SELECT sls_ord_num,
				sls_pred_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
				END AS sls_order_dt,
				CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
				END AS sls_ship_dt,
				CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE)
				END AS sls_due_dt,
				CASE WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
				CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
				END AS sls_price
		FROM bronze.crm_sales_details ORDER BY sls_quantity DESC
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND,@starttime,@endtime)AS NVARCHAR) + ' seconds'

		PRINT 'Loading into ERP Silver'
		PRINT '###################################################################'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>>Inserting Data into silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)

		SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
				END AS cid,
				CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate,
				CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					ELSE 'n/a'
				END
		FROM bronze.erp_cust_az12
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND,@starttime,@endtime)AS NVARCHAR) + ' seconds'

		PRINT '###################################################################'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>>Inserting data into silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)

		SELECT REPLACE(cid,'-','') cid,
				CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
					WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
					ELSE cntry
				END AS cntry
		FROM bronze.erp_loc_a101 
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND,@starttime,@endtime) AS NVARCHAR) + ' seconds'


		PRINT '###################################################################'
		SET @starttime = GETDATE()
		PRINT '>>Truncating silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>>Inserting data into silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
		)

		SELECT id,
				cat,
				subcat,
				maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @endtime = GETDATE()
		PRINT 'Total time taken: ' + CAST(DATEDIFF(SECOND,@starttime,@endtime)AS NVARCHAR) + ' seconds'
		PRINT 'END OF OPERATION'
		SET @endbatchtime = GETDATE()
		PRINT 'Operation time: ' + CAST(DATEDIFF(SECOND,@startbatchtime,@endbatchtime)AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT 'SORRY THERE WAS AN ERROR'
		PRINT 'Error Message: ' + ERROR_MESSAGE()
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER()AS NVARCHAR)
		PRINT 'Error State: ' + CAST(ERROR_STATE()AS NVARCHAR)
		PRINT '==========================================================================================';
	END CATCH
END
