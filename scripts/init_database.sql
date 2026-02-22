

-- this is init file


create database datawarehouse;


use datawarehouse;

create schema bronze;
go
create schema silver;
go -- go is just a seperator here first execute it completely and then execute another
create schema gold;
go
