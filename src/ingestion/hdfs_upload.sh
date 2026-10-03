#!/bin/bash
# =============================================================================
# Ingestion Script: Upload Smart Meter Data to 3-Node HDFS Cluster
# =============================================================================
set -e

HDFS_DATA_DIR="/data/smart_meters/bangalore"
LOCAL_CSV="/workspace/dataset/bangalore_smart_meters_clean.csv"

echo "================================================================="
echo "       HDFS DISTRIBUTED INGESTION & REPLICATION VERIFICATION"
echo "================================================================="

# 1. Create HDFS directories
echo "[*] Creating target HDFS directories..."
hdfs dfs -mkdir -p $HDFS_DATA_DIR
hdfs dfs -mkdir -p /data/output/mapreduce
hdfs dfs -mkdir -p /data/output/spark_ml

# 2. Upload file to HDFS with replication factor = 2
echo "[*] Uploading cleaned Bangalore smart meter data to HDFS..."
hdfs dfs -D dfs.replication=2 -put -f $LOCAL_CSV $HDFS_DATA_DIR/

# 3. Verify directory contents and file sizes in HDFS
echo "[*] Verifying HDFS contents:"
hdfs dfs -ls -h $HDFS_DATA_DIR

# 4. Check File System Block Distribution across Worker1 and Worker2
echo "[*] Inspecting HDFS block distribution and replica locations:"
hdfs fsck $HDFS_DATA_DIR/bangalore_smart_meters_clean.csv -files -blocks -locations

# 5. Cluster Report
echo "[*] Running cluster summary report:"
hdfs dfsadmin -report

echo "[+] Data successfully ingested into HDFS cluster!"
