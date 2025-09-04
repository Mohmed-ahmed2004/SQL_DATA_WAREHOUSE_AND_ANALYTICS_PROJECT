/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

Create or alter procedure bronze.load_bronz as
begin

	declare @start_time datetime,@end_time datetime
	set @start_time= getdate();
	begin try
		print '==========================================';
		print 'loading bronze layer';
		print '==========================================';

		print '--------------------------------------';
		print'loading crm tables';
		print '--------------------------------------';

		set @start_time= getdate();
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
		print'======================================================'


		set @start_time= getdate();
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
		print'======================================================'


		set @start_time= getdate();
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
		print'======================================================'

		print '--------------------------------------';
		print'loading erp tables';
		print '--------------------------------------';

		set @start_time= getdate();
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
		print'======================================================'


		set @start_time= getdate();
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
		print'======================================================'


		set @start_time= getdate();
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\ZBooK\Desktop\Data analysis\SQL With Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow =2,
			fieldterminator=',',
			tablock
		);
		set @end_time= getdate();
		print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'

		print'======================================================'
	 end try
 begin  catch 
 print '********************************************'
  print 'error occured during storde procedure '
  print 'error message'+ error_message();
  print 'error message'+ error_message();
  print 'error number'+ cast(error_number() as varchar);
  print 'error state'+ cast(error_state() as varchar);
  print 'error line'+ cast(error_line() as varchar);
 print '********************************************'
 end catch
 		set @end_time= getdate();
		print'duration of loding bronze layer'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
 end

 exec bronze.load_bronz
