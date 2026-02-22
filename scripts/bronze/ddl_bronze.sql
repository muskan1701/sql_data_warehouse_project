-- to check if table exist

if object_id('bronze.crm_cust_info' ,'U') is not null --'U' is for user defined table as an object
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date );


create table bronze.crm_prd_info (
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);


create table bronze.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_num nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);

create table bronze.erp_loc_a101
(
cid nvarchar (50),
cntry nvarchar (50)
);


create table bronze.erp_cust_az12
(
cid nvarchar (50),
bdate date,
gen nvarchar (50)
);

create table bronze.erp_px_cat_g1v2
(
id nvarchar (50),
cat nvarchar (50),
subcat nvarchar(50),
maintenance nvarchar(50)
);


------------------------------------------------------------------------------
we are going to use bulk insert as its very fast insert data in one go.
--------------------------------------------------------------------------------

*/
create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime , @end_time datetime,@final_start datetime,@final_end datetime --it is to find the the total time taken by each table to load and the entire procedure
	begin try
		set @final_start =getdate();
		print '======================================================';
		print 'loading bronze layer';
		print '======================================================';
		print '------------------------------------------------------';
		print 'loading crm tables';
		print '------------------------------------------------------';
		set @start_time =getdate();
		print 'truncate table'
		print '------------------------------------------------------';
	
		truncate table bronze.crm_cust_info;--to make sure that earlier data is removed 
		print '------------------------------------------------------';
		print 'inserting data'
		print '------------------------------------------------------';
		bulk insert  bronze.crm_cust_info
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
		print '------------------------------------------------------';
		print 'truncate table'
		print '------------------------------------------------------';
	
		set @start_time =getdate();
		truncate table bronze.crm_prd_info;
		print '------------------------------------------------------';
		print 'inserting data'
		print '------------------------------------------------------';
		bulk insert  bronze.crm_prd_info
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
		print '------------------------------------------------------';
		print 'truncate table'
		print '------------------------------------------------------';
		set @start_time =getdate();
		truncate table bronze.crm_sales_details;
		print '------------------------------------------------------';
		print 'inserting data'
		print '------------------------------------------------------';
		bulk insert  bronze.crm_sales_details
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
		print '------------------------------------------------------';
		print 'loading erp tables';
		print '------------------------------------------------------';
	
		print '------------------------------------------------------';
		print 'truncate table'
		print '------------------------------------------------------';
		set @start_time =getdate();
		truncate table bronze.erp_cust_az12;
		print '------------------------------------------------------';
		print 'inserting data'
		print '------------------------------------------------------';
		bulk insert  bronze.erp_cust_az12
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
		print '------------------------------------------------------';
		print 'truncate table'
		print '------------------------------------------------------';
		set @start_time =getdate();
		truncate table bronze.erp_loc_a101;
		bulk insert  bronze.erp_loc_a101
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
		print '------------------------------------------------------';
		print 'truncate table'
		print '------------------------------------------------------';
		set @start_time =getdate();
		truncate table bronze.erp_px_cat_g1v2;
		print '------------------------------------------------------';
		print 'inserting data'
		print '------------------------------------------------------';
		bulk insert  bronze.erp_px_cat_g1v2
		from 'C:\Users\MUSKAN JAIN\Downloads\sql-data-warehouse-project_extracted\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' --do mention .csv or so on
		with (
			FIRSTROW =2,-- firstrow is second as firstrow is header in data file
			FIELDTERMINATOR =',',
			TABLOCK --it is the option to improve performance where we are locking entire table during loading
		);
		set @end_time =getdate();
		print '------------------------------------------------------';
		print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
		print '------------------------------------------------------';

		set @final_end =getdate();
		print 'final time difference is :'+ cast(datediff(second,@final_start,@final_end) as nvarchar)+ 'seconds'
		print '------------------------------------------------------';
	end try
	begin catch
	print '------------------------------------------------------';
	print 'error occured during loading bronze layer'
	print 'error message'+ error_message();
	print 'error message'+ cast(error_number() as nvarchar);
	print 'error message'+ cast (error_state() as nvarchar);
	print '------------------------------------------------------';
	end catch
end
