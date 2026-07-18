-- Create Users
CREATE USER ranger WITH PASSWORD 'ranger_password';
CREATE USER hive WITH PASSWORD 'hive_password';
CREATE USER atlas WITH PASSWORD 'atlas_password';

-- Create Databases with assigned owners
CREATE DATABASE ranger_db OWNER ranger;
CREATE DATABASE metastore OWNER hive;
CREATE DATABASE atlas_db OWNER atlas;

