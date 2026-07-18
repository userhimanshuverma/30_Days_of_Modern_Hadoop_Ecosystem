-- ==========================================================
-- 🛠️ Day 26 Hands-On Lab: Hive Security and Lineage SQL Queries
-- ==========================================================

-- 1. Create a Secure Database
CREATE DATABASE IF NOT EXISTS financial_lake
LOCATION 'hdfs:///financial_lake';

USE financial_lake;

-- 2. Create the Raw Transactions Table (contains PII and PCI-DSS data)
CREATE TABLE IF NOT EXISTS raw_transactions (
    account_id STRING,
    customer_name STRING,
    card_number STRING,
    transaction_date STRING,
    amount DOUBLE,
    country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 3. Populate Sample Data
-- (Simulates financial streams ingested into the lake)
INSERT INTO raw_transactions VALUES 
('ACC123', 'John Doe', '4111-2222-3333-4444', '2026-07-18', 1250.50, 'US'),
('ACC456', 'Jane Smith', '5555-6666-7777-8888', '2026-07-18', 8400.00, 'US'),
('ACC789', 'Alice Johnson', '3782-8224-6310-0014', '2026-07-17', 15.20, 'CA'),
('ACC321', 'Bob Martin', '4992-8224-6310-9999', '2026-07-16', 310.00, 'UK');

-- 4. Create Aggregated Summary Table (Triggers Atlas Lineage Generation)
-- This CTAS (Create Table As Select) statement reads from raw_transactions,
-- aggregates the total transaction amount, and masks the sensitive card_number column
-- by omitting it or hashing it, outputting a safe schema.
CREATE TABLE transactions_summary AS
SELECT 
    country,
    COUNT(account_id) as total_tx_count,
    SUM(amount) as total_tx_volume
FROM raw_transactions
GROUP BY country;

-- 5. Test Access Controls (Ranger Authorization & Masking Verification)
-- Run as unauthorized user (e.g. 'analyst'):
-- Expected behavior: Ranger blocks this query because 'analyst' cannot read raw card numbers.
SELECT customer_name, card_number, amount FROM raw_transactions;

-- Run as authorized user (e.g. 'compliance_officer'):
-- Expected behavior: Allowed, showing full details.
SELECT * FROM raw_transactions;

-- Run aggregate report query:
-- Expected behavior: Ranger allows 'analyst' group to read this aggregate data.
SELECT * FROM transactions_summary;
