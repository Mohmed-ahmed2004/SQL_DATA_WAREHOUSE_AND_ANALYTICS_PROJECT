/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


use master 
go

--create database called 'DataWarehouse'
drop database if exists DataWarehouse
	create database DataWarehouse;
	go

use DataWarehouse;

-- create the schemas 
	go
	create schema bronze;
	go
	create schema silver;
	go 
	create schema gold;
	go


