#!/bin/bash
# =============================================================================
# Run Hadoop MapReduce Streaming Job on 3-Node Cluster via YARN
# =============================================================================
set -e

STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
fi

INPUT_PATH="/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv"
OUTPUT_PATH="/data/output/mapreduce/peak_demand_summary"

echo "[*] Cleaning previous MapReduce output directory in HDFS..."
hdfs dfs -rm -r -f $OUTPUT_PATH || true

echo "[*] Submitting MapReduce Streaming Job to YARN..."
hadoop jar $STREAMING_JAR \
    -D mapreduce.job.name="Bangalore_SmartMeter_Hourly_Peak_Aggregation" \
    -D mapreduce.job.reduces=2 \
    -files /workspace/src/processing/mapper.py,/workspace/src/processing/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input $INPUT_PATH \
    -output $OUTPUT_PATH

echo "[+] MapReduce Job Completed on YARN!"
echo "[*] Displaying partial results from distributed HDFS output:"
hdfs dfs -cat $OUTPUT_PATH/part-* | head -n 25

echo "[*] Saving output to local workspace output folder..."
hdfs dfs -getmerge $OUTPUT_PATH /workspace/output/mapreduce_results.txt
echo "[+] Results successfully exported to output/mapreduce_results.txt"
