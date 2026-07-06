-- Hive Query Execution and Optimization script for Day 15 Lab

-- =========================================================================
-- 1. STAGING DATA IMPORT
-- =========================================================================
INSERT OVERWRITE TABLE default.sample_lines VALUES 
('apache tez is a dag engine'),
('hadoop mapreduce writes to disk between jobs'),
('tez pipelines tasks in memory'),
('hive on tez is faster than hive on mapreduce'),
('distributing analytics pipelines on yarn');

-- =========================================================================
-- 2. GENERATE BENCHMARK RECORDS (20,000 ROWS)
-- =========================================================================
INSERT OVERWRITE TABLE default.benchmark_data
SELECT row_number() over() as id, 
       concat('value_', cast(rand()*100 as int)) as value 
FROM default.sample_lines t1 
CROSS JOIN default.sample_lines t2 
CROSS JOIN default.sample_lines t3 
CROSS JOIN default.sample_lines t4 
CROSS JOIN default.sample_lines t5
LIMIT 20000;

-- =========================================================================
-- 3. BENCHMARK QUERY: HIGH LEVEL AGGREGATIONS
-- =========================================================================
-- Executing the query on MapReduce
SET hive.execution.engine=mr;
SELECT value, count(*) as cnt, avg(id) as avg_id 
FROM default.benchmark_data 
GROUP BY value 
ORDER BY cnt DESC 
LIMIT 10;

-- Executing the query on Apache Tez
SET hive.execution.engine=tez;
SELECT value, count(*) as cnt, avg(id) as avg_id 
FROM default.benchmark_data 
GROUP BY value 
ORDER BY cnt DESC 
LIMIT 10;

-- =========================================================================
-- 4. OPTIMIZED TEZ MAP-SIDE JOIN DEMO
-- =========================================================================
-- Configure Tez Map Join to broadcast the small tables automatically
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000;

-- Sample Join Query executing in a single DAG stage with Broadcast Edge
SELECT a.value, s.line 
FROM default.benchmark_data a
JOIN default.sample_lines s ON (a.value LIKE concat('%', s.line, '%'))
LIMIT 10;
