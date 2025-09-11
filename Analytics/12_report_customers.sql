/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/
create or alter view gold.Customer_Report as


with base_query as

/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------*/

(
SELECT 
	c.customer_key,
	c.customer_number,
	concat(firstname,' ',lastname) customer_name,
	country, 
	gender,
	DATEDIFF(year,birthdate,getdate()) age,
	f.order_number,
	f.product_key,
	order_date,
	f.quantity,
	f.sales_amount

from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
where order_date is not null 
),

/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
intermediate_query as
(
select
	customer_key,
	customer_number,
	customer_name,
	country, 
	gender,
	age,
	count( distinct order_number) total_order ,
	sum(sales_amount) total_sales ,
	count(quantity) total_quntity,
	count(product_key) total_product,
	max(order_date)last_order,
	datediff(month,min(order_date),max(order_date)) life_span

from base_query

group by 
	customer_key,
	customer_number,
	customer_name,
	country, 
	gender,
	age)


	select 
		customer_key,
	customer_number,
	customer_name,
	country, 
	gender,
	age,
	total_order ,
	 total_sales ,
	 total_quntity,
	 total_product,
	last_order,
	 life_span,
        CASE 
            WHEN life_span >= 12 AND total_sales > 5000 THEN 'VIP'
            WHEN life_span >= 12 AND total_sales <= 5000 THEN 'Regular'
         ELSE 'New'
		 end customer_segment,
		 case 
		 when age<20 then 'under 20'
		 when age between 20 and 29 then '20-29'
		 when age between 30 and 39 then '30-39'
		 when age between 40 and 49 then '40-49'
		 when age between 50 and 59 then '50-59'
		 else '60 and above'
		 end age_group ,
		 datediff(MONTH,last_order,GETDATE()) recency ,
		 case when total_order=0 then 0
		else  total_sales/total_order 
		end average_order_value,

		case when life_span=0 then 0
		else  total_sales/life_span 
		end as average_monthly_spend
	from intermediate_query
	
	select *from gold.Customer_Report
