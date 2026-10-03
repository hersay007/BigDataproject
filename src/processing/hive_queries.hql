-- ============================================================================
-- Apache Hive: OLAP Schema & Analytical Queries for Bangalore Smart Grid
-- Stored on Distributed HDFS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS bangalore_energy_db;
USE bangalore_energy_db;

-- 1. Create External Table mapped directly to HDFS storage
DROP TABLE IF EXISTS raw_smart_meters;
CREATE EXTERNAL TABLE raw_smart_meters (
    meter_id STRING,
    meter_timestamp STRING,
    consumer_type STRING,
    substation_id STRING,
    area_name STRING,
    discom_zone STRING,
    active_energy_kwh DOUBLE,
    reactive_energy_kvarh DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_factor DOUBLE,
    frequency_hz DOUBLE,
    anomaly_flag INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smart_meters/bangalore'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Partitioned & ORC-Optimized Analytical Table
CREATE TABLE IF NOT EXISTS partitioned_smart_meters (
    meter_id STRING,
    meter_timestamp STRING,
    consumer_type STRING,
    substation_id STRING,
    area_name STRING,
    active_energy_kwh DOUBLE,
    reactive_energy_kvarh DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_factor DOUBLE,
    frequency_hz DOUBLE
)
PARTITIONED BY (discom_zone STRING)
STORED AS ORC;

-- 3. OLAP Analytical Query 1: Top 5 Highest Consuming Substations during Peak Hours
SELECT 
    substation_id,
    area_name,
    discom_zone,
    ROUND(SUM(active_energy_kwh), 2) AS total_mwh_consumed,
    ROUND(AVG(power_factor), 3) AS avg_power_factor,
    ROUND(MAX(current_a), 2) AS peak_line_current
FROM raw_smart_meters
GROUP BY substation_id, area_name, discom_zone
ORDER BY total_mwh_consumed DESC
LIMIT 5;

-- 4. OLAP Analytical Query 2: Peak Hour Load Analysis with Window Ranking (DENSE_RANK)
SELECT 
    substation_id,
    consumer_type,
    SUBSTR(meter_timestamp, 12, 2) AS hour_of_day,
    ROUND(SUM(active_energy_kwh), 2) AS hourly_kwh,
    DENSE_RANK() OVER (PARTITION BY consumer_type ORDER BY SUM(active_energy_kwh) DESC) as demand_rank
FROM raw_smart_meters
GROUP BY substation_id, consumer_type, SUBSTR(meter_timestamp, 12, 2);

-- 5. OLAP Analytical Query 3: Low Power Factor (<0.85) Grid Inefficiency Detection
SELECT 
    substation_id,
    area_name,
    COUNT(*) AS low_pf_events,
    ROUND(AVG(voltage_v), 2) AS avg_grid_voltage,
    ROUND(AVG(power_factor), 3) AS avg_pf
FROM raw_smart_meters
WHERE power_factor < 0.85
GROUP BY substation_id, area_name
HAVING COUNT(*) > 50
ORDER BY low_pf_events DESC;
