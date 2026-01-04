/*
***************************************
	  CREATE DATABASE & SCHEMAS
***************************************
Script Purpose:
	This script creates a new database named 'Datawarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	with in the DATABASE: 'bronze','silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'Datawarehouse' DATABASE if it exists.
	All data in the DATABASE will permanetly deleted. Proceed with caution and
	ensure you have the proper backups before running this script.
*/

-- DROP Datawarehouse IF it EXISTS

IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO
-- CREATE Datawarehouse DATABASE
CREATE DATABASE Datawarehouse;
USE Datawarehouse;
GO
-- CREATE SCHEMAS 

-- CREATE bronze SCHEMA
CREATE SCHEMA bronze;
GO

-- CREATE silver SCHEMA
CREATE SCHEMA silver;
GO

-- CREATE gold SCHEMA
CREATE SCHEMA gold;
