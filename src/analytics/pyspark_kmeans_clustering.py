#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, stddev, count, round as spark_round, substring
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def run_kmeans_load_profiling():
    print("[*] Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("Bangalore_SmartMeter_KMeans_Clustering") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    dataset_path = "dataset/bangalore_smart_meters_clean.csv"
    if not os.path.exists(dataset_path):
        dataset_path = "hdfs://localhost:9000/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv"

    print("[*] Reading telemetry records from: " + str(dataset_path))

    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(dataset_path)

    total_count = df.count()
    print("[+] Total records loaded: {0:,}".format(total_count))

    # Extract hour from timestamp: "YYYY-MM-DD HH:MM:SS" -> chars 12 to 13
    df_with_hour = df.withColumn("hour", substring(col("timestamp"), 12, 2))

    print("[*] Performing distributed feature engineering per substation & hour...")
    hourly_features = df_with_hour.groupBy("substation_id", "consumer_type", "hour").agg(
        spark_round(avg("active_energy_kwh"), 4).alias("avg_kwh"),
        spark_round(max("active_energy_kwh"), 4).alias("max_kwh"),
        spark_round(avg("power_factor"), 4).alias("avg_pf"),
        spark_round(stddev("active_energy_kwh"), 4).alias("consumption_stddev"),
        count("active_energy_kwh").alias("total_readings")
    ).withColumn("load_factor", spark_round(col("avg_kwh") / (col("max_kwh") + 0.0001), 4)).na.fill(0)

    hourly_features.show(5, truncate=False)

    feature_cols = ["avg_kwh", "max_kwh", "avg_pf", "consumption_stddev", "load_factor"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled_df = assembler.transform(hourly_features)

    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)

    k = 3
    print("[*] Training Spark MLlib KMeans model with k=3...")
    kmeans = KMeans(featuresCol="features", predictionCol="cluster_id", k=k, seed=42)
    model = kmeans.fit(scaled_df)

    predictions = model.transform(scaled_df)

    evaluator = ClusteringEvaluator(predictionCol="cluster_id", featuresCol="features")
    silhouette = evaluator.evaluate(predictions)
    print("\n=======================================================")
    print("[+] Clustering Complete! Silhouette Score = {0:.4f}".format(silhouette))
    print("=======================================================\n")

    centers = model.clusterCenters()
    print("[*] Cluster Centers (Normalized):")
    for idx, center in enumerate(centers):
        print("  - Cluster {0}: {1}".format(idx, [round(float(c), 4) for c in center]))

    print("\n[*] Cluster Population Breakdown:")
    predictions.groupBy("cluster_id", "consumer_type").count().orderBy("cluster_id").show()

    spark.stop()
    print("[+] PySpark MLlib Pipeline Finished Successfully!")

if __name__ == "__main__":
    run_kmeans_load_profiling()
