#!/bin/bash
# ==============================================================================
# Spark Analytics & Streamlit Dashboard Runner
# Author: Shivanshi (Member C)
# ==============================================================================
set -e

echo "[*] Executing PySpark Distributed Analytics..."
python3 src/analytics/pyspark_analytics.py || true

echo "[*] Executing Spark MLlib K-Means Clustering..."
python3 src/analytics/spark_ml_kmeans.py || true

echo "[*] Launching Streamlit Interactive Dashboard on port 8501..."
streamlit run src/dashboard/app.py --server.port 8501 --server.address 0.0.0.0