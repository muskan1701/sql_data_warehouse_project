/* 
we need to first exlore data in the bronze layer

explore tables and its insights 

1.) [bronze].[crm_cust_info] holds customer info 
2.)[bronze].[crm_prd_info] has current and history product info
3>) [bronze].[crm_sales_details] transactional records and sales and orders (we can join prd_key from here to prd table,
								similarly we can join cst_id with cst_cust_info table.)

4.)[bronze].[erp_cust_az12] : can be join via cust_id
5.)[bronze].[erp_loc_a101]	: can be join via cust_id
6.)[bronze].[erp_px_cat_g1v2]	: can be join id via product_key

================================================================================================================
we are going to use full load 
=================================================================================================================
metadata columns
===============================================================================================================
extra columns created by data engineers that do no originate from the source data
for examples : create date,update date ,etc
===============================================================================================================
*/
/*

if object_id('silver.crm_cust_info' ,'U') is not null --'U' is for user defined table as an object
	drop table silver.crm_cust_info;
create table silver.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate() --to create date automatocally
);


if object_id('silver.crm_prd_info' ,'U') is not null --'U' is for user defined table as an object
	drop table silver.crm_prd_info;
create table silver.crm_prd_info (
prd_id int,
cat_id nvarchar(50),

prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);

if object_id('silver.crm_sales_details' ,'U') is not null --'U' is for user defined table as an object
				drop table silver.crm_sales_details;
			create table silver.crm_sales_details (
			sls_ord_num nvarchar(50),
			sls_prd_key nvarchar(50),
			sls_cust_id int,
			sls_order_dt date,
			sls_ship_dt date,
			sls_due_dt date,
			sls_sales int,
			sls_quantity int,
			sls_price int,
			dwh_create_date datetime2 default getdate()
			);

create table silver.erp_loc_a101
(
cid nvarchar (50),
cntry nvarchar (50),
dwh_create_date datetime2 default getdate()
);


create table silver.erp_cust_az12
(
cid nvarchar (50),
bdate date,
gen nvarchar (50),
dwh_create_date datetime2 default getdate()
);

create table silver.erp_px_cat_g1v2
(
id nvarchar (50),
cat nvarchar (50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
);

*/
