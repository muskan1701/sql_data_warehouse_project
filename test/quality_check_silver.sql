
/*
checking problem with bronze table data 
===================================================
check for null,duplicates in primary key 
-------------------------------------------------------
select cst_id,count(*) from bronze.crm_cust_info
group by cst_id 
having count (*)>1 or cst_id is null--cst_id is null
--This finds records where customer ID is missing.

---------------------------------------------------
--check for unwanted  spaces

select cst_firstname from bronze.crm_cust_info 
where cst_firstname != trim(cst_firstname)

select cst_lastname from bronze.crm_cust_info 
where cst_lastname != trim(cst_lastname)

select cst_gndr from bronze.crm_cust_info 
where cst_gndr != trim(cst_gndr)
--------------------------------------------------------------------------
--we will check cardinality of the columns , we will give full name
select distinct cst_gndr
from bronze.crm_cust_info 

-----------------------------------------------------------------------------
make sure date is not string
-----------------------------------------------------------------------------------
this will be done in every table now 
----------------------------------------------------------------------------
select prd_id,count(*) from bronze.crm_prd_info
group by prd_id 
having count (*)>1 or prd_id is null
--------------------------------------------------------------------------
--check for unwanted  spaces

select prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm)
---------------------------------------------------------------------------------
to check negative prices or cost
select prd_cost 
from bronze.crm_prd_info
where prd_cost <0 or prd_cost is null
---------------------------------------------------------------------------------
to check quality of start and end time start date should be less than end date

select * from bronze.crm_prd_info
where prd_end_dt <prd_start_dt

--------------------------------------------------------------------------------------------
third table
-------------------------------------------------------------------------------------------------
to convert date type integer to date
--checking 0 and converting into null
select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 or len(sls_order_dt) !=8 or sls_order_dt >20500101 or sls_order_dt<19000101
--checking if value of date is higher than 2050 year or less than certain year
----------------------------------------------------------------------------------------------
order date must earlier than shipping date and due date
------------------------------------------------------------------------------------------------
select 
*
from bronze.crm_sales_details
where sls_order_dt >sls_ship_dt or sls_order_dt >sls_due_dt
--------------------------------------------------------------------------------------------------------------------------
-- business rules : sales =quantity *price

select 
distinct sls_sales,sls_quantity,sls_price
from bronze.crm_sales_details
where sls_sales !=sls_quantity *sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price

-we will calculate sales for null =qt*price and similarly sls_price =sales/qt


-----------------------------------------------------------------------------------------------------------
4TH table
-----------------------------------------------------------------------------------------------------------
to check if every cid start from aws something

select 
cid, bDate,gen from bronze.erp_cust_az12
where cid like '%AW00011000%'
-----------------------------------------------------------------------
to check old dates  or date is less than current date 
----------------------------------------------
select distinct bdate from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()
------------------------------------------------------------------------------------------------
check gender column

select distinct gen
from bronze.erp_cust_az12
------------------------------------------------------------------------------------------------------

5th table 
=====================================================================================================
select cid, cntry from bronze.erp_loc_a101;

select replace (cid,'-','') cid
from bronze.erp_loc_a101
where cid not in 
(select cst_key from silver.crm_cust_info)


select distinct cntry from bronze.erp_loc_a101s
==========================================================================================================
6 th table
----------------------------------------------------------------------------------------------------------
select 
id,
cat,
subcat,
maintenance from bronze.erp_px_cat_g1v2
-- check unwanted spaces

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat !=(subcat) or maintenance != trim(maintenance)
--check distinct
select distinct cat from bronze.erp_px_cat_g1v2
*/

