-- Hive DDL Schema Initialization File for Day 15 Lab

-- Drop tables if they exist
DROP TABLE IF EXISTS default.sample_lines;
DROP TABLE IF EXISTS default.benchmark_data;

-- 1. Create staging table for text line imports
CREATE TABLE default.sample_lines (
    line STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 2. Create the larger benchmark table
CREATE TABLE default.benchmark_data (
    id INT,
    value STRING
)
STORED AS ORC; -- Using ORC format for optimized columnar execution in Tez
