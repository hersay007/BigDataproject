#!/bin/bash
set -e

INPUT_HDFS="/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv"
OUTPUT_HDFS="/data/output/mapreduce/peak_demand_summary"
STREAMING_JAR=$(find /opt/hadoop-3.2.1/share/hadoop/tools/lib -name "hadoop-streaming-*.jar" | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
    echo "[!] Hadoop streaming jar not found, searching root..."
    STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
fi

echo "[*] Cleaning previous MapReduce output directory in HDFS..."
hdfs dfs -rm -r -f $OUTPUT_HDFS || true

echo "[*] Submitting MapReduce Streaming Job to YARN..."
hadoop jar $STREAMING_JAR \
    -D mapreduce.job.name="BangaloreEnergy_PeakDemandAggregator" \
    -D mapreduce.job.reduces=2 \
    -files /workspace/src/processing/mapper.py,/workspace/src/processing/reducer.py \
    -mapper "/usr/bin/python3 mapper.py" \
    -reducer "/usr/bin/python3 reducer.py" \
    -input $INPUT_HDFS \
    -output $OUTPUT_HDFS

echo "[+] MapReduce Job Completed Successfully!"
echo "[*] Sample Aggregated Output from HDFS:"
hdfs dfs -cat $OUTPUT_HDFS/part-00000 | head -n 25

mkdir -p /workspace/output
hdfs dfs -getmerge $OUTPUT_HDFS /workspace/output/mapreduce_results.txt
echo "[+] Merged MapReduce output saved locally to output/mapreduce_results.txt"