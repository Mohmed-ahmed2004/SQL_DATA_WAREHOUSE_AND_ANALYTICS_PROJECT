/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

	--check for unwanted spaces
	--expectation :no result 
	select *
	from silver.crm_cust_info
	where cst_firstname!= trim(cst_firstname)
	or cst_lastname!= trim(cst_lastname)

	--data standardization & consistency 
	select distinct cst_marital_status
	from silver.crm_cust_info

	select distinct cst_gndr
	from silver.crm_cust_info

		--check for null or duplicate in primary key
	--expectation :no result 

	select cst_id,
	count(*)
	from silver.crm_cust_info
	group by cst_id
	having count(*)!=1 or cst_id is null
--================================================
--================================================
--================================================

	--check for null or duplicate in primary key
	--expectation :no result 

	select prd_id,
	count(*)
	from bronze.crm_prd_info
	group by prd_id
	having count(*)!=1 or prd_id is null

	--check for unwanted spaces
	--expectation :no result 
	select *
	from bronze.crm_prd_info
	where  prd_nm!=trim(prd_nm)

	--check for null or negative number
	--expectation :no result 
	select prd_cost
	from silver.crm_prd_info
	where prd_cost  is null or prd_cost <0

	--data standardization & consistency 
	select distinct prd_line
	from silver.crm_prd_info

	--check for invalid date orders 
	--expectation :no result 
	select *
	from silver.crm_prd_info
	where prd_end_dt<prd_start_dt

		--check for invalid date orders 
	--expectation :no result 
	select *
	from silver.crm_sales_details
	where sls_order_dt >sls_due_dt or  sls_order_dt > sls_ship_dt

	--check data consistency between: sls_sales ,sls_quantity,sls_price 
	--expectation :no result 
		select distinct
		sls_sales   ,
		sls_quantity,
		sls_price 
		from silver.crm_sales_details
		where sls_sales!=sls_quantity * sls_price
		or sls_sales is null or sls_quantity is null or sls_price is null
		or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
		order by sls_sales,sls_quantity,sls_price

--================================================
--================================================
--================================================
	--identify out of range dates
	--expectation:no result
		select * 
		from silver.erp_cust_az12
		where BDATE> getdate()

	--date standardization & consistency
		select distinct GEN
		from silver.erp_cust_az12
--================================================
--================================================
--================================================
		--date standardization & consistency
		select distinct CNTRY 	
		FROM silver.erp_loc_a101
--=================================================
--=================================================
	--check for unwanted spaces
	--expectation :no result 
	select * 
	from silver.erp_px_cat_g1v2
	where CAT!=trim(cat) or subcat!=trim(subcat) or MAINTENANCE!=trim(MAINTENANCE)

	--date standardization & consistency
	select distinct MAINTENANCE
	from silver.erp_px_cat_g1v2
