/* ==========================================================================================
Procedure Name : bronze.load_bronze
Purpose        : Load raw source data (CRM & ERP) into the Bronze layer
Layer          : Bronze (Raw / Landing Layer)

Instructions:
1. Ensure all source CSV files exist in the specified file paths.
2. SQL Server service account must have READ access to the source directories.
3. Target bronze tables must already exist with correct schemas.
4. This procedure truncates existing data before loading (FULL REFRESH).
5. Execute during low-usage periods to minimize locking impact.

Execution:
EXEC bronze.load_bronze;

Author         : Capwell Murimi
Last Updated   : 2026
========================================================================================== */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '===================================================';
        PRINT 'Inserting data into bronze layer';
        PRINT '===================================================';

        /* =========================
           Loading CRM Data
        ========================= */
        PRINT 'Loading CRM';
        PRINT '----------------------------------------------------';

        /* CRM CUSTOMER INFO */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        /* CRM PRODUCT INFO */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting data into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        /* CRM SALES DETAILS */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        /* =========================
           Loading ERP Data
        ========================= */
        PRINT 'Loading ERP';
        PRINT '----------------------------------------------------';

        /* ERP CUSTOMER */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        /* ERP LOCATION */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting data into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        /* ERP PRODUCT CATEGORY */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\CapwellTheNerd\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>>----------------------------------------------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '>> Loading bronze layer completed successfully';
        PRINT '>> Whole batch load time: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '==========================================================================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================================================================';
    END CATCH
END;
