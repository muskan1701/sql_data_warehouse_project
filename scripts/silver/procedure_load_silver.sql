
create or alter procedure silver.load_silver as 
begin
declare @start_time datetime , @end_time datetime,@final_start datetime,@final_end datetime --it is to find the the total time taken by each table to load and the entire procedure
	begin try
			print '------------------------------------------------------';
			print 'loading silver layer '
			print '------------------------------------------------------';
			set @final_start =getdate();
			print '------------------------------------------------------';
			print 'truncate table 1'
			print '------------------------------------------------------';
			set @start_time =getdate();
			truncate table silver.crm_cust_info;
			print '------------------------------------------------------';
			print 'inserting table 1'
			print '------------------------------------------------------';

			insert into silver.crm_cust_info(cst_id,
			cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,
			cst_create_date)

			select cst_id,
			cst_key,
			trim (cst_firstname) as cst_firstname,
			trim (cst_lastname) as cst_lastname,

			case when upper(trim(cst_marital_status)) ='S' then 'Single'
				 when upper(trim(cst_marital_status)) ='M' then 'Married'
				 else 'n/a'
			end
			cst_marital_status,
			case when upper(trim(cst_gndr)) ='F' then 'Female'
				 when upper(trim(cst_gndr)) ='M' then 'Male'
				 else 'n/a'
			end cst_gndr,
			cst_create_date
			from(
			select *,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as flaglast 
			from bronze.crm_cust_info)t
			where flaglast = 1
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';

			-- we are creating flag on the basis of date we will only take latest date data that will be flagged as 1 ,this will remove 
			-- duplicates
			--this for 1st table
			--select * from silver.crm_cust_info
			print '------------------------------------------------------';
			print 'truncate table 2'
			print '------------------------------------------------------';
			set @start_time =getdate();
			truncate table silver.crm_prd_info;
			print '------------------------------------------------------';
			print 'inserting table 2'
			print '------------------------------------------------------';
			insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
			)
			select 
			prd_id ,
			replace(substring(prd_key,1,5),'-','_') as cat_id,
			--as prd key is combination of multiple things hence we will extract  further we will use this key to join table from erp 
			-- hence right now it is CO-RF , it should be CO_RF to match it.
			substring(prd_key,7,len(prd_key)) as prd_key,
			-- this will help us to join with sales table
			prd_nm ,
			isnull (prd_cost,0) as prd_cost ,

			case upper(trim(prd_line)) 
					when 'R' then 'Road'
				 when 'M' then 'Mountain'
				 when 'S' then 'Other Sales'
				 when 'T' then 'Touring'
				 else 'n/a'
			end
			prd_line ,
			cast(prd_start_dt as date) as prd_start_dt,  --we are going to clean it by taking startdate of second column as end date for first
			--if start date is not less than end date ,
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt )-1 as date ) as prd_end_dt 

			from bronze.crm_prd_info
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';


			--select * from silver.crm_prd_info

			-- this for 2nd table
					
			print '------------------------------------------------------';
			print 'truncate table 3'
			print '------------------------------------------------------';
			set @start_time =getdate();
			truncate table silver.crm_sales_details;
			print '------------------------------------------------------';
			print 'inserting table 3'
			print '------------------------------------------------------';
			insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt,
			sls_sales ,
			sls_quantity ,
			sls_price )
			select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id ,

			case when sls_order_dt =0 or len(sls_order_dt) !=8 then null
				 else cast(cast(sls_order_dt as varchar)as date)
				 end --date cant be directly casted from int to date ,1st varchar and then date
				 sls_order_dt ,
			case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
				 else cast(cast(sls_ship_dt as varchar)as date)
				 end as sls_ship_dt	,
			case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
				 else cast(cast(sls_due_dt as varchar)as date)
				 end as sls_due_dt ,

			case when sls_sales is null or 
				sls_sales <=0 or 
				sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price)
				else sls_sales
				 end as sls_sales  ,
			sls_quantity ,
			case when sls_price is null or sls_price <=0 
					then sls_sales / nullif(sls_quantity,0)
				else sls_price
				 end as sls_price 
			from bronze.crm_sales_details
			-- to check integrity of that column 
			--where sls_prd_key not in (select prd_key from silver.crm_prd_info)
			--where sls_cust_id not in (select cst_id from silver.crm_cust_info)
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';


			print '------------------------------------------------------';
			print 'truncate table 4'
			print '------------------------------------------------------';
			set @start_time =getdate();
			truncate table silver.erp_cust_az12;
			print '------------------------------------------------------';
			print 'inserting table 4'
			print '------------------------------------------------------';
			insert into silver.erp_cust_az12(cid, bdate,gen)
			select 

			case when cid like 'NAS%' THEN SUBSTRING (cid, 4,len(cid))
				 else cid
			end cid,
			case when bdate > GETDATE() then null 
				 else bdate
			end bdate,
			case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
					when upper(trim(gen)) in ('M','MALE') THEN 'Male'
				 else 'n/a'
			end gen 
			from bronze.erp_cust_az12
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';
			print '------------------------------------------------------';
			print 'truncate table 5'
			print '------------------------------------------------------';

			set @start_time =getdate();
			truncate table silver.erp_loc_a101;
			print '------------------------------------------------------';
			print 'inserting table 5'
			print '------------------------------------------------------';
			insert into silver.erp_loc_a101 (cid,cntry)
			select replace (cid,'-','') cid,
			case when trim(cntry) = 'DE' THEN 'Germany'
				when trim(cntry) in ('US','USA') THEN 'United States'
				when trim(cntry) = '' or cntry is null THEN 'n/a'
				else trim(cntry)
			end  cntry
			from bronze.erp_loc_a101
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';

			print '------------------------------------------------------';
			print 'truncate table 6'
			print '------------------------------------------------------';
			truncate table  silver.erp_px_cat_g1v2;
			print '------------------------------------------------------';
			print 'inserting table 6'
			print '------------------------------------------------------';
			insert into silver.erp_px_cat_g1v2 (id,
			cat,
			subcat,
			maintenance) 
			select 
			id,
			cat,
			subcat,
			maintenance from bronze.erp_px_cat_g1v2
			set @end_time =getdate();
			print '------------------------------------------------------';
			print 'load difference is :'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
			print '------------------------------------------------------';
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


--exec silver.load_silver 

/*
insert into ... select : table must exist
select id into tablea from table b :creates table and insert data 
order of column matter
*/
