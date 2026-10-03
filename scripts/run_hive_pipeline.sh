#!/bin/bash
# ==============================================================================
# Automated Hive Pipeline & OLAP Query Execution Script
# Author: Udayan Bhargava (Member B)
# ==============================================================================
set -e

echo "[*] Initializing Hive Metastore and executing analytical queries..."
hive -f /workspace/src/processing/hive_queries.hql

echo "[*] Exporting Substation Aggregation Summary to output/hive_analysis_report.csv..."
mkdir -p /workspace/output
hive -e "USE bangalore_energy_db; 
SELECT substation_id, consumer_type, 
       ROUND(AVG(voltage_v), 2) AS avg_voltage, 
       ROUND(AVG(power_factor), 3) AS avg_pf, 
       ROUND(SUM(active_energy_kwh), 2) AS total_kwh 
FROM raw_smart_meters 
GROUP BY substation_id, consumer_type;" | tr '\t' ',' > /workspace/output/hive_analysis_report.csv

echo "[+] Hive OLAP analytical pipeline completed successfully!"