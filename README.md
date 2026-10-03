# Distributed Energy Consumption Analytics & Demand Pattern Mining
## Big Data Systems Project | 3-Node Hadoop & Apache Spark Cluster

### Project Overview
This project demonstrates an enterprise-grade distributed big data processing pipeline for smart grid smart-meter telemetry in Bangalore, India. 
The system runs across a **3-node cluster (1 Master, 2 DataNodes/Workers)** to perform distributed storage on **Hadoop HDFS**, distributed batch aggregation with **Hadoop MapReduce**, OLAP data warehousing using **Apache Hive**, and distributed machine learning via **Apache Spark MLlib** (K-Means customer load profiling and Anomaly Detection for power theft/meter tampering).

---

### Team Members & Defined Roles
| Member | Operating System | Defined Project Role | Key Contributions |
| :--- | :--- | :--- | :--- |
| **Member A** (Lead) | macOS (Apple Silicon / Intel) | **Cluster Architect & Ingestion Lead** | Multi-node Docker setup, HDFS cluster provisioning, Kaggle dataset pipeline, replication tuning & HDFS verification. |
| **Member B** | Windows 11 (PowerShell/WSL2) | **MapReduce & Hive Data Engineer** | Python Streaming MapReduce for peak demand aggregation, Hive schema DDL & external table partitioning, analytical OLAP queries. |
| **Member C** | Windows 11 (PowerShell/WSL2) | **Spark MLlib & Analytics Lead** | PySpark MLlib distributed K-Means clustering, energy theft anomaly detection, Spark job execution on YARN, performance benchmarks. |

---

### Cluster Topology (3 Nodes)
- **Master Node (`master`, 172.20.0.10)**:
  - HDFS NameNode (Port 9870 / 9000)
  - YARN ResourceManager (Port 8088)
  - Spark Master (Port 8080 / 7077)
  - Hive Metastore & HiveServer2 (Port 10000)
- **Worker Node 1 (`worker1`, 172.20.0.11)**:
  - HDFS DataNode 1 (Port 9864)
  - YARN NodeManager 1 (Port 8042)
  - Spark Worker 1 (Port 8081)
- **Worker Node 2 (`worker2`, 172.20.0.12)**:
  - HDFS DataNode 2 (Port 9865)
  - YARN NodeManager 2 (Port 8043)
  - Spark Worker 2 (Port 8082)

---

### Dataset Details
- **Source**: Kaggle "Smart Energy Meters in Bangalore India" (`unseemlycoder/smart-energy-meters-in-bangalore-india`)
- **Scale**: Scaled to millions of interval telemetry rows (15-minute / hourly granularity).
- **Attributes**: Meter_ID, Timestamp, Active_Energy_kWh, Reactive_Energy_kVARh, Voltage_V, Current_A, Power_Factor, Frequency_Hz, Substation_ID, Consumer_Type.

---

### Repository Structure
```text
BigDataProject/
├── README.md
├── docker-compose.yml
├── dataset/
│   ├── download_kaggle.sh
│   ├── generate_large_smart_meter_data.py
│   └── bangalore_smart_meters.csv
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   └── mapred-site.xml
├── src/
│   ├── ingestion/
│   │   ├── preprocess.py
│   │   └── hdfs_upload.sh
│   ├── processing/
│   │   ├── mapper.py
│   │   ├── reducer.py
│   │   ├── run_mapreduce.sh
│   │   └── hive_queries.hql
│   └── analytics/
│       ├── pyspark_kmeans_clustering.py
│       └── pyspark_anomaly_detection.py
├── scripts/
│   ├── cluster_health_check.sh
│   └── run_all_pipeline.sh
└── output/
    ├── mapreduce_results.txt
    ├── hive_analysis_report.csv
    ├── kmeans_cluster_centers.json
    └── anomaly_detections.csv
```
