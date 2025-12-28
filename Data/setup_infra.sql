--- This script is used as a backup for the RBAC hierarchy

--- Using super user account
USE ROLE ACCOUNTADMIN;


--- Creating the dbt_wh to do the transformations

CREATE WAREHOUSE DBT_WH WITH warehouse_size = 'x-small' auto_suspend = 60;


--- Creating the database (where all the transformations happen)
CREATE DATABASE COMPLAINTS_DB;

--- Creating the schemas that serve as the medallion architecture

CREATE SCHEMA COMPLAINTS_RAW;

CREATE SCHEMA COMPLAINTS_SILVER;

CREATE SCHEMA COMPLAINTS_GOLD;

--- Creating the role to do the transformations
CREATE ROLE DBT_ROLE;

--- Grant the following permissions to the roles and users:
--  - usage of the warehouse to dbt_role
--  - all permissions of the database to dbt_role
--  - just to avoid further problems, create tables on schemas to dbt_role
--  - grant role to user

GRANT USAGE ON WAREHOUSE DBT_WH TO DBT_ROLE;

GRANT ALL ON DATABASE COMPLAINTS_DB TO DBT_ROLE;

GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_RAW TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_SILVER TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_GOLD TO ROLE DBT_ROLE;

GRANT ROLE DBT_ROLE TO USER SHADOW75098;


