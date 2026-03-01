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
create view gold.dim_customers as 
select 
	ROW_NUMBER () over(order by cst_id) as customer_key,
	ci.cst_id  ,
	ci.cst_key customer_number,
	ci.cst_firstname first_name,
	ci.cst_lastname  last_name,
	ci.cst_marital_status marital_status ,
	ci.cst_create_date ,
	ca.bdate birthdate,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce (ca.gen,'n/a') --we are converting null ingto na or else it will take qvalue from second table 
	end as new_gender ,
	la.cntry country
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key=la.cid




	select * from gold.dim_customers



