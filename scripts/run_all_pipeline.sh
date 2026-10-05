#!/bin/bash
# =============================================================================
# Master End-to-End Pipeline Execution Script
# Orchestrates Ingestion -> MapReduce -> Hive -> PySpark MLlib
# =============================================================================
set -e

echo "================================================================="
echo "   STARTING END-TO-END DISTRIBUTED BIG DATA PROCESSING PIPELINE"
echo "================================================================="

# Step 1: Health Check
/workspace/scripts/cluster_health_check.sh

# Step 2: Ingestion & Distributed HDFS Upload
echo -e "\n[*] Step 2: Ingesting dataset into HDFS..."
/workspace/src/ingestion/hdfs_upload.sh

# Step 3: Run Distributed MapReduce Job
echo -e "\n[*] Step 3: Executing Hadoop MapReduce job on YARN..."
/workspace/src/processing/run_mapreduce.sh

# Step 4: PySpark K-Means Load Profiling
echo -e "\n[*] Step 4: Executing PySpark MLlib K-Means clustering..."
python3 /workspace/src/analytics/pyspark_kmeans_clustering.py

# Step 5: PySpark Anomaly & Electricity Theft Detection
echo -e "\n[*] Step 5: Executing PySpark Grid Anomaly detection..."
python3 /workspace/src/analytics/pyspark_anomaly_detection.py

echo -e "\n================================================================="
echo "   COMPLETE BIG DATA ANALYTICS PIPELINE FINISHED SUCCESSFULLY!"
echo "================================================================="
