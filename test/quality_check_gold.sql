




/*

here we will be creating the views .
to chech if any duplicates are there or not 
select cst_id ,count(*) from
(
	select 
	ci.cst_id  ,
	ci.cst_key customer_number,
	ci.cst_firstname first_name,
	ci.cst_lastname  last_name,
	ci.cst_marital_status marital_status ,
	ci.cst_gndr,
	ci.cst_create_date ,
	ca.bdate birthdate,
	ca.gen,
	la.cntry country
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key=la.cid
)t group by cst_id
having count(*)>1



*/
/*
to resolve integration isssues of column gender which is coming from two tables 
select distinct 
	ci.cst_gndr,
	ca.gen,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce (ca.gen,'n/a') --we are converting null ingto na or else it will take qvalue from second table 
		end as new_gender 
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
	order  by 1,2
	-- issues here is sometime there are two different values here ,sometimes there is 
	-- data from first table but not from second table'
	--data from first table is not available but available in second table
	-- when data is different from the those two tables then we have to decide which one is the master table for accurate data
*/

/*
1.)give nice friendly names to columns
2>)arrange order of the columns 

3.)we have to check whether facts(transaction) /dimension( descriptive info)
if dimension table we have to have primary key if not then generate surrogate key

surrogate key: system - generated unique identifier assigned to each record in a table. it is majorly used for  
only for connecting data model. it has no meaning to a business table.

*/

/*
here we can make two types of views either historical or either current .
we are doing on current 
--check uniqueness

select prd_key ,count(*) from 
(
select p.prd_id ,
p.cat_id ,
p.prd_key ,
p.prd_nm ,
p.prd_cost ,
p.prd_line,
p.prd_start_dt,
px.cat,
px.subcat,
px.maintenance
from silver.crm_prd_info p
left join silver.erp_px_cat_g1v2 px
on p.cat_id = px.id
where p.prd_end_dt is null --filter out all historical data 
)t group by prd_key
having count (*)>1
-- it is a dim table we will create surrogate key



*/
/*
this is third views 
this table is fact table 
here we have to use dimension's surrogate keys instead of id's to easily connect fatcs with dimensions 
this process is called as data lookup 

select
s.sls_ord_num ,
pr.p_key, --here we are using dimension surrogate key instead od id's
			s.sls_cust_id,
			c.cst_id,--as there is some mismatch in data hence i will be using 
			--this key for joining instead of surrogate key
			c.customer_key,--here we are using dimension surrogate key instead od id's
			s.sls_order_dt ,
			s.sls_ship_dt ,
			s.sls_due_dt ,
			s.sls_sales ,
			s.sls_quantity,
			s.sls_price 
from silver.crm_sales_details s
left join gold.dim_products pr 
on s.sls_prd_key =pr.prd_key
left join gold.dim_customers c
on s.sls_cust_id =c.cst_id

-- here we are building facts table


create view gold.fact_sales as 
select
s.sls_ord_num ,
pr.p_key, --here we are using dimension surrogate key instead od id's
			s.sls_cust_id,
			c.cst_id,--as there is some mismatch in data hence i will be using 
			--this key for joining instead of surrogate key
			c.customer_key,--here we are using dimension surrogate key instead od id's
			s.sls_order_dt ,
			s.sls_ship_dt ,
			s.sls_due_dt ,
			s.sls_sales ,
			s.sls_quantity,
			s.sls_price 
from silver.crm_sales_details s
left join gold.dim_products pr 
on s.sls_prd_key =pr.prd_key
left join gold.dim_customers c
on s.sls_cust_id =c.cst_id

-- check data quality
-- check if all dimension tables can successfully join to the fact table
--foreign key integrity

select * from gold.fact_sales f
left join gold.dim_customers c 
on f.customer_key =c.customer_key
left join gold.dim_products p
on p.p_key =f.p_key
where p.p_key is null

*/
