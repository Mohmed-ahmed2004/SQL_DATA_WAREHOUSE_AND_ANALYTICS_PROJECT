/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
create or alter view gold.Product_Report as

with base_query as
(
select 
	f.order_number,
	f.order_date,
	f.sales_amount,
	f.quantity,
	f.customer_key,
	p.product_key,
	p.product_number,
	p.product_name,
	p.categoy,
	p.subcategory,
	p.product_cost
	from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key=p.product_key
	where order_date is not null
),
/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
product_Aggregation as
(
select 
	product_key,
	product_number,
	product_name,
	categoy,
	subcategory,
	product_cost,
	count (distinct order_number) total_order,
	sum(sales_amount)total_sales,
	sum(quantity) total_quantity,
	COUNT(distinct customer_key) total_customer,
	MAX(order_date) last_order,
	datediff(month ,min(order_date),max(order_date))life_span
from base_query
group by 
product_key,
product_number,
product_name,
categoy,
subcategory,
product_cost
)
/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/


select 
	product_key,
	product_number,
	product_name,
	categoy,
	subcategory,
	product_cost,
	 total_order,
	total_sales,
	 total_quantity,
	 total_customer,
     last_order,
	life_span,
	case 
	when total_sales>50000 then 'high preformance'
	when total_sales>=10000 then 'mid preformance'
	else 'low preformance'
	end Segments_products,
	datediff(month,last_order,GETDATE())recency,
	case 
	when total_order=0 then 0
	else  total_sales/total_order 
	end average_order_revenue,
	case 
	when life_span=0 then 0
    else  total_sales/life_span
	end average_monthly_revenue
	

from  product_Aggregation

select * from gold.Product_Report
