/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
create or alter procedure silver.load_silver as
begin

		declare @start_time datetime,@end_time datetime
		set @start_time= getdate();
		begin try
			print '==========================================';
			print 'loading silver layer';
			print '==========================================';

			print '--------------------------------------';
			print'loading crm tables';
			print '--------------------------------------';


		set @start_time= getdate();
		truncate table silver.crm_cust_info 
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select
		cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
			case
		when trim (upper(cst_marital_status))='S' then 'Single'
		when trim (upper(cst_marital_status))='M' then 'Married'
		else 'Unknow'
		end cst_marital_status,
		case
		when trim (upper(cst_gndr))='M' then 'Male'
		when trim (upper(cst_gndr))='F' then 'Female'
		else 'Unknow'
		end cst_gndr,
		cst_create_date
		from
		(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as flag
		from bronze.crm_cust_info
		where cst_id is not null
		)f  where flag=1 

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
			print'======================================================'

	--=================================================
	--=================================================
	
		set @start_time= getdate();
		truncate table silver.crm_prd_info
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		select
		prd_id,
		replace (substring(prd_key,1,5),'-','_' )cat_id,
		substring(prd_key,7,len(prd_key))prd_key,
		prd_nm,
		coalesce(prd_cost,0)prd_cost ,
		case
		when trim(upper(prd_line)) ='M' then 'Mountain'
		when trim(upper(prd_line)) ='R' then 'Road'
		when trim(upper(prd_line)) ='S' then 'Other sales'
		when trim(upper(prd_line)) ='T' then 'Touring'
		else 'Unknow'
		end prd_line,
		prd_start_dt,
		dateadd(day,-1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) ) as prd_end_dt
		from bronze.crm_prd_info

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
			print'======================================================'
	--=================================================
	--=================================================
		set @start_time= getdate();
		truncate table silver.crm_sales_details
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales ,
			sls_quantity,
			sls_price

		)

		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case
				when sls_order_dt =0 or len(sls_order_dt)!=8 then null
				else cast( cast(sls_order_dt as varchar )as date)
			end sls_order_dt,

			case
				when sls_ship_dt =0 or len(sls_ship_dt)!=8 then null
				else cast( cast(sls_ship_dt as varchar )as date)
			end sls_ship_dt,
	
			case
				when sls_due_dt=0 or len(sls_due_dt)!=8 then null
				else cast( cast(sls_due_dt as varchar )as date)
			end sls_due_dt,

			case 
				when sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity* abs (sls_price)
				then sls_quantity* abs (sls_price)
				else sls_sales
			end sls_sales ,
			sls_quantity,
			case 
				when sls_price is null or sls_price<=0
				then sls_sales/nullif (sls_quantity,0)
				else sls_price
			end sls_price
		from bronze.crm_sales_details

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
			print'======================================================'

	--================================================
	--================================================

			print '--------------------------------------';
			print'loading crm tables';
			print '--------------------------------------';

			set @start_time= getdate();
			truncate table silver.erp_cust_az12
			insert into silver.erp_cust_az12(cid,bdate,gen)
			select 
			case 
			when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			else cid 
			end cid,
			case 
			when BDATE> getdate() then null
			else bdate
			end bdate ,
			case 
			when upper(trim (gen)) in('M','MALE') then 'Male' 
			when upper(trim (gen)) in('F','FEMALE') then 'Female' 
			else 'Unknow'
			end gen
			from bronze.erp_cust_az12

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
			print'======================================================'
	--===========================================
	--===========================================
			set @start_time= getdate();
			truncate table silver.erp_loc_a101
			insert into silver.erp_loc_a101(cid,cntry)

			SELECT 
			Replace( CID,'-','') cid ,
			case
			when upper(trim(CNTRY)) ='DE' OR upper(trim(CNTRY))='GERMANY' then 'Germany'
			when upper(trim(CNTRY)) ='US' OR upper(trim(CNTRY))='USA' OR upper(trim(CNTRY)) ='UNITED STATES'
			THEN 'Untied states'
			when upper(trim(CNTRY)) ='AUSTRAliA' THEN 'Australia' 
			when upper(trim(CNTRY)) ='CANADA' THEN 'Canada' 
			when upper(trim(CNTRY)) ='FRANCE' THEN 'France'
			when upper(trim(CNTRY)) ='UNITED KINGDOM' THEN 'United Kingdom' 
			else 'Unknow'
			end cntry
			FROM bronze.erp_loc_a101

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'
			print'======================================================'

	--==================================
	--==================================

			set @start_time= getdate();
			truncate table silver.erp_px_cat_g1v2 
			insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
			select id,
			cat,
			subcat,
			MAINTENANCE
			from bronze.erp_px_cat_g1v2

			set @end_time= getdate();
			print'load duration'+ cast( datediff(second,@start_time, @end_time) as varchar)+' seconds'


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
end

exec silver.load_silver;

