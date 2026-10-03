"""
PySpark Distributed Analytics Engine for Bangalore Smart Grid Telemetry.
Performs distributed aggregation, time-series windowing, and peak demand profiling.
Author: Shivanshi (Member C)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max, min as spark_min, round, hour, to_timestamp

def create_spark_session():
    return SparkSession.builder \
        .appName("BangaloreEnergy_DistributedAnalytics") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

def run_analytics(spark, input_path, output_dir):
    print(f"[*] Reading telemetry dataset from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 1. Total energy consumption by Substation and Consumer Type
    substation_summary = df.groupBy("substation_id", "area_name", "consumer_type") \
        .agg(
            round(spark_sum("active_energy_kwh"), 2).alias("total_kwh"),
            round(avg("voltage_v"), 2).alias("mean_voltage"),
            round(spark_max("current_a"), 2).alias("peak_current"),
            round(avg("power_factor"), 3).alias("mean_power_factor")
        ).orderBy(col("total_kwh").desc())
    
    print("[+] Substation Energy & Power Quality Summary:")
    substation_summary.show(10, truncate=False)
    substation_summary.write.mode("overwrite").parquet(f"{output_dir}/substation_summary.parquet")

    # 2. Hourly Grid Load Profile
    df_with_hour = df.withColumn("hour", hour(to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")))
    hourly_load = df_with_hour.groupBy("hour", "consumer_type") \
        .agg(
            round(avg("active_energy_kwh"), 3).alias("avg_hourly_kwh"),
            round(spark_max("active_energy_kwh"), 3).alias("peak_hourly_kwh")
        ).orderBy("hour")
    
    print("[+] Diurnal Hourly Grid Load Curve:")
    hourly_load.show(24, truncate=False)
    hourly_load.write.mode("overwrite").parquet(f"{output_dir}/hourly_load_profile.parquet")

if __name__ == "__main__":
    spark = create_spark_session()
    run_analytics(spark, "/workspace/dataset/bangalore_smart_meters_clean.csv", "/workspace/output/analytics")
    spark.stop()
    