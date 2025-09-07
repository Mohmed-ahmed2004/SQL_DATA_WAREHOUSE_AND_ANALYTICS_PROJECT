/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


--====================================
--create dimension view for customers
--====================================
create or alter   view gold.dim_customers as 
select 
	row_number() over(order by ci.cst_id) customer_key,
	ci.cst_id customer_id,
	ci.cst_key customer_number,
	ci.cst_firstname firstname,
	ci.cst_lastname lastname ,
	la.CNTRY country,
	ci.cst_marital_status marital_status,
		CASE
		when ci.cst_gndr!='Unknow'then ci.cst_gndr
		else coalesce(ca.GEN,'Unknow')
		end gender,
	ca.BDATE birthdate,
	ci.cst_create_date create_date 
from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca on ci.cst_key=ca.CID
	left join silver.erp_loc_a101 la on ci.cst_key=la.CID

--====================================
--create dimension view for product
--====================================

create or alter view gold.dim_products as 
select 
	ROW_NUMBER() over(order by prd_start_dt, prd_key) product_key,
	pn.prd_id product_id,
	pn.prd_key product_number,
	pn.prd_nm product_name,
	pn.cat_id category_id,
	pc.CAT categoy,
	pc.SUBCAT subcategory,
	pc.MAINTENANCE,
	pn.prd_cost product_cost,
	pn.prd_line product_line,
	pn.prd_start_dt start_date

from silver.crm_prd_info pn
	left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id=pc.ID
	where pn.prd_end_dt is null -- filte out all historical data (use only current data)

--====================================
--create fact view for sales
--====================================
 
 create or alter view gold.fact_sales as
 SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
