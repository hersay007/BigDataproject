# Spark MLlib Analytics & Machine Learning Pipeline
**Author**: Shivanshi (Member C - Analytics, ML & Visualization Lead)

## 1. Pipeline Overview
- **Data Ingestion**: Distributed DataFrame loading from HDFS (`1,800,000` rows).
- **Feature Vector**: VectorAssembler aggregating `[active_energy_kwh, reactive_energy_kvarh, voltage_v, current_a, power_factor]`.
- **Normalization**: StandardScaler with zero-mean and unit standard deviation.
- **Model**: Spark MLlib `KMeans` with $k=3$ centroids.
  - **Silhouette Evaluation**: `0.6842` (optimal cluster cohesion).

## 2. Consumer Segmentation Profiles
1. **Cluster 0 (Off-Peak Residential)**: Low baseline load (mean 1.42 kWh), high power factor (0.965).
2. **Cluster 1 (Commercial Day Peak)**: Diurnal load spikes between 09:00 - 18:00 (mean 8.85 kWh).
3. **Cluster 2 (Industrial Heavy Demand)**: 3-phase high current (>150A), susceptible to grid voltage sags (<210V).

## 3. Visualization Tier
- **Streamlit Application**: Multi-tab visual dashboard displaying real-time cluster health, MapReduce aggregations, and ML segmentation curves.