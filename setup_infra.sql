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
--  - grant all permissions of the database to dbt_role
--  - grant create tables on schemas to dbt_role
--  - grant role to user

-- usage of the warehouse to dbt_role
GRANT USAGE ON WAREHOUSE DBT_WH TO DBT_ROLE;


-- grant all permissions of the database to dbt_role
GRANT ALL ON DATABASE COMPLAINTS_DB TO DBT_ROLE;

-- grant create tables on schemas to dbt_role
GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_RAW TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_SILVER TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_GOLD TO ROLE DBT_ROLE;

--- Grant usages of the schemas to dbt role
GRANT USAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_RAW TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_SILVER TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_GOLD TO ROLE DBT_ROLE;

--- Grant create to stages of the schemas to dbt role
GRANT CREATE STAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_RAW TO ROLE DBT_ROLE;
GRANT CREATE STAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_SILVER TO ROLE DBT_ROLE;
GRANT CREATE STAGE ON SCHEMA COMPLAINTS_DB.COMPLAINTS_GOLD TO ROLE DBT_ROLE;

--- Grant all privileges of the schemas to dbt role
GRANT ALL PRIVILEGES ON SCHEMA COMPLAINTS_DB.COMPLAINTS_RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA COMPLAINTS_DB.COMPLAINTS_SILVER TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA COMPLAINTS_DB.COMPLAINTS_GOLD TO ROLE DBT_ROLE;

-- Finally grant role to users

GRANT ROLE DBT_ROLE TO USER SHADOW75098;


