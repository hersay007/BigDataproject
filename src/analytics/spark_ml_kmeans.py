"""
Spark MLlib K-Means Clustering & Power Anomaly Detection Pipeline.
Clusters consumers into load profiles and detects grid anomalies (under-voltage, phase overload).
Author: Shivanshi (Member C)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def run_ml_pipeline():
    spark = SparkSession.builder \
        .appName("BangaloreEnergy_SparkML_KMeans") \
        .getOrCreate()

    print("[*] Loading Smart Grid telemetry for unsupervised ML clustering...")
    df = spark.read.csv("/workspace/dataset/bangalore_smart_meters_clean.csv", header=True, inferSchema=True)

    # Feature Engineering Vector
    feature_cols = ["active_energy_kwh", "reactive_energy_kvarh", "voltage_v", "current_a", "power_factor"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    feature_df = assembler.transform(df)

    # Standardize features (zero mean, unit variance)
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(feature_df)
    scaled_df = scaler_model.transform(feature_df)

    # Train K-Means (k=3: Off-Peak Base Load, Moderate Load, Industrial Peak Load)
    kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster_id")
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)

    # Evaluate Clustering Silhouette Score
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster_id", metricName="silhouette")
    silhouette = evaluator.evaluate(predictions)
    print(f"[+] Spark MLlib K-Means Silhouette Score: {silhouette:.4f}")

    # Cluster Centers
    print("[+] Cluster Centroids:")
    for i, center in enumerate(model.clusterCenters()):
        print(f"  Cluster {i}: {center}")

    # Grid Anomaly Detection: Voltage sag (<210V) or Poor Power Factor (<0.80)
    anomalies = predictions.withColumn(
        "is_anomaly",
        when((col("voltage_v") < 210.0) | (col("power_factor") < 0.80), 1).otherwise(0)
    )
    anomaly_count = anomalies.filter(col("is_anomaly") == 1).count()
    total_count = df.count()
    print(f"[+] Total Grid Anomaly Intervals Detected: {anomaly_count} / {total_count} ({anomaly_count/total_count*100:.2f}%)")

    # Save summary
    anomalies.groupBy("cluster_id", "consumer_type").count().show()
    spark.stop()

if __name__ == "__main__":
    run_ml_pipeline()