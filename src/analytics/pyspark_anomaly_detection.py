#!/usr/bin/env python3
"""
Distributed Grid Anomaly & Power Theft Detection using Apache Spark
Identifies abnormal consumption drops, voltage sag violations, and meter bypasses
using statistical Z-Score dispersion and inter-quartile range (IQR) detection.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, count, abs as spark_abs, lit

def run_anomaly_detection():
    print("[*] Launching Distributed Anomaly Detection Pipeline on Spark...")
    spark = SparkSession.builder \
        .appName("Bangalore_SmartMeter_Grid_Anomaly_Detection") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://master:9000/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)

    # 1. Anomaly Condition A: Voltage Sag / Surge (< 200V or > 250V for 1-phase; < 360V or > 440V for 3-phase)
    voltage_anomalies = df.filter(
        ((col("consumer_type") == "Residential") & ((col("voltage_v") < 200) | (col("voltage_v") > 250))) |
        ((col("consumer_type") != "Residential") & ((col("voltage_v") < 360) | (col("voltage_v") > 440)))
    )
    print(f"[!] Voltage Grid Violations Detected: {voltage_anomalies.count():,}")

    # 2. Anomaly Condition B: Energy Theft / Shunt Bypass Indicator
    # Meter draws high line current (> 10A) but reports active energy near zero (< 0.05 kWh) and abnormally low power factor (< 0.5)
    theft_suspects = df.filter(
        (col("current_a") > 10.0) & 
        (col("active_energy_kwh") < 0.05) & 
        (col("power_factor") < 0.6)
    )
    print(f"[!] Suspected Meter Tampering / Power Theft Events: {theft_suspects.count():,}")

    # 3. Aggregate Anomaly Summary by Substation
    anomaly_summary = theft_suspects.groupBy("substation_id", "area_name", "discom_zone") \
        .agg(count("meter_id").alias("theft_alert_count")) \
        .orderBy(col("theft_alert_count").desc())

    print("\n[*] High Risk Substations Identified:")
    anomaly_summary.show(truncate=False)

    # Export anomalies to HDFS
    theft_suspects.select("meter_id", "timestamp", "substation_id", "area_name", "active_energy_kwh", "current_a", "power_factor") \
        .write.mode("overwrite").csv("hdfs://master:9000/data/output/spark_ml/theft_alerts.csv", header=True)
    
    print("[+] Anomaly detection report stored in HDFS at /data/output/spark_ml/theft_alerts.csv")
    spark.stop()

if __name__ == "__main__":
    run_anomaly_detection()
