/*
=====================================================================
Create Database and Schemas
=====================================================================
Purpose:
	This creates a new DataWarehouse named SalesWarehouse after checking if it exists.
	If it exists, it drops the database and recreate it. It also creates three schemas
	within database "bronze", "silver", "gold"

Warning: 
	Executing this script will delete the contents of pre existing database 
	named SalesWarehouse if it exists.

*/

USE master;
GO

-- Check whether database exists or not
IF EXISTS (SELECT 1 from sys.databases WHERE name = 'SalesWarehouse')
BEGIN
	DROP DATABASE SalesWarehouse;
END;
GO

-- Create Database SalesWarehouse
CREATE DATABASE SalesWarehouse;
GO

USE SalesWarehouse;
GO

-- Create Schema for each layer of architecture.
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;






