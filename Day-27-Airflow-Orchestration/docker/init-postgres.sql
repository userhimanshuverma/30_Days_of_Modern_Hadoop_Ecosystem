-- docker/init-postgres.sql
-- Setup additional schemas and permissions in PostgreSQL for Hadoop services
-- Note: 'airflow_db' is automatically created by the postgres Docker container because of POSTGRES_DB configuration.

-- Create Metastore User and Database
CREATE USER hive WITH PASSWORD 'hive_secure_pass';
CREATE DATABASE metastore_db OWNER hive;
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;

-- Create Hue/Analytics Console User and Database
CREATE USER hue WITH PASSWORD 'hue_secure_pass';
CREATE DATABASE hue_db OWNER hue;
GRANT ALL PRIVILEGES ON DATABASE hue_db TO hue;
