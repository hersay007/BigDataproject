# DISTRIBUTED ENERGY CONSUMPTION ANALYTICS AND DEMAND PATTERN MINING USING HADOOP AND SPARK
## Term End Project Evaluation - Big Data Systems Laboratory
**Topic**: Large-scale smart meter data stored across a 3-node Hadoop HDFS cluster with MapReduce, Hive OLAP, and Spark MLlib.
**Technologies**: Hadoop HDFS, Hadoop MapReduce, Apache Spark / PySpark, Spark MLlib, Hive, Docker, Python 3, GitHub.

---

## 1. TEAM MEMBERS & DEFINED ROLES
| Role | Student Name | Roll No | GitHub Username | OS & Hardware | Key Responsibilities |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Member A (Cluster Lead)** | Student A | 1MS21CS001 | aditya-cluster-lead | macOS (M2 Mac) | Docker 3-node cluster orchestration, HDFS replication setup, Kaggle Bangalore dataset acquisition, HDFS block distribution verification. |
| **Member B (Data Engineer)** | Student B | 1MS21CS042 | bhavna-data-eng | Windows 11 (PowerShell/WSL2) | Hadoop Streaming MapReduce (mapper.py, reducer.py) for substation hourly aggregation, Hive external table DDL, and OLAP window queries. |
| **Member C (Spark ML Lead)** | Student C | 1MS21CS089 | chirag-spark-ml | Windows 11 (PowerShell) | Distributed PySpark MLlib K-Means clustering, customer load profiling, power theft anomaly detection, and master automation script. |

---

## 2. 3-NODE CLUSTER TOPOLOGY
- **Master Node (`master`, 172.20.0.10)**:
  - HDFS NameNode (Port 9870 / 9000)
  - YARN ResourceManager (Port 8088)
  - Spark Master (Port 8080 / 7077)
  - Hive Metastore (Port 10000)
  - Allocated RAM: 4096 MB
- **Worker Node 1 (`worker1`, 172.20.0.11)**:
  - HDFS DataNode 1 (Port 9864)
  - YARN NodeManager 1 (Port 8042)
  - Spark Worker 1 (Port 8081, 2 cores)
  - Allocated RAM: 4096 MB
- **Worker Node 2 (`worker2`, 172.20.0.12)**:
  - HDFS DataNode 2 (Port 9865)
  - YARN NodeManager 2 (Port 8043)
  - Spark Worker 2 (Port 8082, 2 cores)
  - Allocated RAM: 4096 MB

**Distributed Storage Configuration**:
- `dfs.replication` = 2 (Blocks replicated across worker1 and worker2)
- `dfs.blocksize` = 134217728 (128 MB)
- `fs.defaultFS` = hdfs://master:9000

---

## 3. DATASET SPECIFICATION
- **Source**: Kaggle "Smart Energy Meters in Bangalore India" (`unseemlycoder/smart-energy-meters-in-bangalore-india`)
- **Telemetry Frequency**: 15-minute sampling interval
- **Attributes**: Meter_ID, Timestamp, Consumer_Type (Residential/Commercial/Industrial), Substation_ID, Area_Name, Active_Energy_kWh, Reactive_Energy_kVARh, Voltage_V, Current_A, Power_Factor, Frequency_Hz, Anomaly_Flag.

---

## 4. EXECUTION PROOFS & SCREENSHOT PLACEHOLDERS (ALL 14 DELIVERABLES)

### SCREENSHOT 1: HDFS DFSADMIN REPORT: 2 LIVE DATANODES AND CONFIGURED CAPACITY
- **Category**: Cluster Setup
- **Objective / Description**: Verifies distributed storage setup across the 3-node cluster. Proves that both worker1 and worker2 are recognized as Live DataNodes with healthy heartbeat intervals.
- **What Evaluator Looks For**: "Live datanodes (2)", worker hostnames (worker1, worker2), Configured Capacity (>50GB), DFS Used %, and zero dead datanodes.
- **Command to Execute**:
```bash
docker exec -it hadoop-master hdfs dfsadmin -report
```
- **Expected Terminal Output Preview**:
```text
Configured Capacity: 120892743680 (112.59 GB)
Present Capacity: 98721456128 (91.94 GB)
DFS Remaining: 98721400000 (91.94 GB)
DFS Used: 56128 (54.81 KB)
DFS Used%: 0.00%
Recheck interval: 300000 ms

Live datanodes (2):
Name: 172.20.0.11:9866 (worker1)
Hostname: worker1
Decommission Status : Normal
Configured Capacity: 60446371840 (56.29 GB)
DFS Used: 28064 (27.41 KB)
Non DFS Used: 11085643776 (10.32 GB)
DFS Remaining: 49360700000 (45.97 GB)
Last contact: Fri Oct 03 14:22:10 UTC 2024

Name: 172.20.0.12:9866 (worker2)
Hostname: worker2
Decommission Status : Normal
Configured Capacity: 60446371840 (56.29 GB)
DFS Used: 28064 (27.41 KB)
Non DFS Used: 11085643776 (10.32 GB)
DFS Remaining: 49360700000 (45.97 GB)
Last contact: Fri Oct 03 14:22:11 UTC 2024
```

> **[ PASTE SCREENSHOT 1 HERE ]**
> *(Capture terminal / web browser showing "Live datanodes (2)", worker hostnames (worker1, worker2), Configured Capacity (>50GB), DFS Used %, and zero dead datanodes. and paste image here)*

---

### SCREENSHOT 2: HADOOP NAMENODE WEB UI OVERVIEW (PORT 9870)
- **Category**: Cluster Setup
- **Objective / Description**: Web dashboard showing NameNode status, active cluster ID, block pool ID, heap memory utilization, and the "Datanodes" tab listing worker1 and worker2.
- **What Evaluator Looks For**: Cluster Summary table showing 2 Active Nodes, NameNode health, SafeMode OFF, and live block count.
- **Command to Execute**:
```bash
open http://localhost:9870/dfshealth.html#tab-datanode
```
- **Expected Terminal Output Preview**:
```text
Browser URL: http://localhost:9870
Page Title: Overview 'master:9000' (active)
Version: 3.2.1
Cluster ID: CID-b570228e-8dbf-47dc-a07e-39ff2e11899e
Block Pool ID: BP-18274092-172.20.0.10-1700000000000
Summary:
Security is OFF. SafeMode is OFF.
Live Nodes: 2
Dead Nodes: 0
Decommissioning Nodes: 0
```

> **[ PASTE SCREENSHOT 2 HERE ]**
> *(Capture terminal / web browser showing Cluster Summary table showing 2 Active Nodes, NameNode health, SafeMode OFF, and live block count. and paste image here)*

---

### SCREENSHOT 3: YARN RESOURCEMANAGER WEB UI (PORT 8088)
- **Category**: Cluster Setup
- **Objective / Description**: YARN ResourceManager node list displaying worker1:8042 and worker2:8042 in "RUNNING" state with 4GB memory and 2 vCores allocated each.
- **What Evaluator Looks For**: Table of Nodes: Node Address (worker1:8042, worker2:8042), Node State (RUNNING), Rack (/default-rack), Available Memory (8192 MB Total).
- **Command to Execute**:
```bash
open http://localhost:8088/cluster/nodes
```
- **Expected Terminal Output Preview**:
```text
Browser URL: http://localhost:8088/cluster/nodes
Total Nodes: 2 Active
Node HTTP Address: worker1:8042 | State: RUNNING | Mem: 4096 MB | vCores: 2
Node HTTP Address: worker2:8042 | State: RUNNING | Mem: 4096 MB | vCores: 2
Active Applications: 0 (Idle standby for jobs)
```

> **[ PASTE SCREENSHOT 3 HERE ]**
> *(Capture terminal / web browser showing Table of Nodes: Node Address (worker1:8042, worker2:8042), Node State (RUNNING), Rack (/default-rack), Available Memory (8192 MB Total). and paste image here)*

---

### SCREENSHOT 4: HDFS BLOCK PLACEMENT & REPLICATION VERIFICATION (HDFS FSCK)
- **Category**: HDFS Storage
- **Objective / Description**: Proof of distributed storage. Shows the smart meter CSV file split across HDFS blocks and replicated across both worker1 and worker2.
- **What Evaluator Looks For**: Block ID (e.g. blk_1073741825_1001), len, replicas=[172.20.0.11:9866, 172.20.0.12:9866], Target Replication=2, Status=HEALTHY.
- **Command to Execute**:
```bash
docker exec -it hadoop-master hdfs fsck /data/smart_meters/bangalore/bangalore_smart_meters_clean.csv -files -blocks -locations
```
- **Expected Terminal Output Preview**:
```text
/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv 74581290 bytes, 1 block(s):  OK
0. BP-18274092-172.20.0.10-1700000000000:blk_1073741825_1001 len=74581290 repl=2 [172.20.0.11:9866, 172.20.0.12:9866]

Status: HEALTHY
 Number of data-nodes:          2
 Total size:                    74581290 B
 Total blocks (validated):      1 (avg. block size 74581290 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    2
 Average block replication:     2.0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Number of data-nodes:          2
```

> **[ PASTE SCREENSHOT 4 HERE ]**
> *(Capture terminal / web browser showing Block ID (e.g. blk_1073741825_1001), len, replicas=[172.20.0.11:9866, 172.20.0.12:9866], Target Replication=2, Status=HEALTHY. and paste image here)*

---

### SCREENSHOT 5: HADOOP MAPREDUCE EXECUTION ON YARN CLUSTER
- **Category**: MapReduce
- **Objective / Description**: Submitting Python Streaming job to YARN. Verifies distributed Map phase (containers on worker1/worker2) and Reduce phase.
- **What Evaluator Looks For**: Job ID (job_..._0001), Map 100% Reduce 100%, Counters: Map input records, Reduce input records, Shuffled Maps.
- **Command to Execute**:
```bash
docker exec -it hadoop-master /workspace/src/processing/run_mapreduce.sh
```
- **Expected Terminal Output Preview**:
```text
INFO client.RMProxy: Connecting to ResourceManager at master/172.20.0.10:8032
INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1728000000000_0001
INFO mapreduce.JobSubmitter: Executing with tokens: []
INFO conf.Configuration: resource-types.xml not found
INFO impl.YarnClientImpl: Submitted application application_1728000000000_0001
INFO mapreduce.Job: The url to track the job: http://master:8088/proxy/application_1728000000000_0001/
INFO mapreduce.Job: Running job: job_1728000000000_0001
INFO mapreduce.Job: Job job_1728000000000_0001 running in uber mode : false
INFO mapreduce.Job:  map 0% reduce 0%
INFO mapreduce.Job:  map 33% reduce 0%
INFO mapreduce.Job:  map 67% reduce 0%
INFO mapreduce.Job:  map 100% reduce 0%
INFO mapreduce.Job:  map 100% reduce 50%
INFO mapreduce.Job:  map 100% reduce 100%
INFO mapreduce.Job: Job job_1728000000000_0001 completed successfully
```

> **[ PASTE SCREENSHOT 5 HERE ]**
> *(Capture terminal / web browser showing Job ID (job_..._0001), Map 100% Reduce 100%, Counters: Map input records, Reduce input records, Shuffled Maps. and paste image here)*

---

### SCREENSHOT 6: MAPREDUCE OUTPUT IN HDFS (HOURLY SUBSTATION PEAK LOAD)
- **Category**: MapReduce
- **Objective / Description**: Inspection of reducer output files in HDFS showing hourly aggregated consumption, maximum current, and average voltage per Bangalore substation.
- **What Evaluator Looks For**: Tab-delimited table with headers: substation_id, consumer_type, hour, total_kwh, max_current_a, avg_voltage_v.
- **Command to Execute**:
```bash
docker exec -it hadoop-master hdfs dfs -cat /data/output/mapreduce/peak_demand_summary/part-00000 | head -n 25
```
- **Expected Terminal Output Preview**:
```text
substation_id   consumer_type   hour    total_kwh       max_current_a   avg_voltage_v   reading_count
SUB_BLR_01      Commercial      09      3420.50         48.20           401.20          1250
SUB_BLR_01      Commercial      10      4190.20         52.80           399.10          1250
SUB_BLR_01      Commercial      14      4850.80         58.40           398.50          1250
SUB_BLR_01      Residential     07      1240.10         18.60           229.40          3120
SUB_BLR_01      Residential     19      2480.90         28.90           226.80          3120
SUB_BLR_02      Industrial      11      18920.00        240.50          402.10          625
SUB_BLR_02      Industrial      15      21400.50        265.10          397.80          625
```

> **[ PASTE SCREENSHOT 6 HERE ]**
> *(Capture terminal / web browser showing Tab-delimited table with headers: substation_id, consumer_type, hour, total_kwh, max_current_a, avg_voltage_v. and paste image here)*

---

### SCREENSHOT 7: APACHE HIVE EXTERNAL TABLE CREATION & PARTITION DDL
- **Category**: Hive OLAP
- **Objective / Description**: Demonstrates Hive OLAP data warehouse integration over HDFS. Shows table metadata pointing to Location hdfs://master:9000/data/smart_meters/bangalore.
- **What Evaluator Looks For**: Table Type: EXTERNAL_TABLE, Location: hdfs://master:9000/data/smart_meters/bangalore, InputFormat: TextInputFormat, Columns match schema.
- **Command to Execute**:
```bash
docker exec -it hadoop-master hive -e "USE bangalore_energy_db; DESCRIBE FORMATTED raw_smart_meters;"
```
- **Expected Terminal Output Preview**:
```text
# Detailed Table Information
Database:               bangalore_energy_db
OwnerType:              USER
CreateTime:             Fri Oct 03 14:35:00 UTC 2024
LastAccessTime:         UNKNOWN
Retention:              0
Location:               hdfs://master:9000/data/smart_meters/bangalore
Table Type:             EXTERNAL_TABLE
Table Parameters:
        EXTERNAL                TRUE
        skip.header.line.count  1

# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
```

> **[ PASTE SCREENSHOT 7 HERE ]**
> *(Capture terminal / web browser showing Table Type: EXTERNAL_TABLE, Location: hdfs://master:9000/data/smart_meters/bangalore, InputFormat: TextInputFormat, Columns match schema. and paste image here)*

---

### SCREENSHOT 8: HIVE ANALYTICAL OLAP QUERY RESULTS (PEAK SUBSTATIONS & WINDOW RANKING)
- **Category**: Hive OLAP
- **Objective / Description**: Execution of analytical aggregation query over HDFS table showing total MWh consumed across the highest demand substations in Bangalore.
- **What Evaluator Looks For**: Hive MapReduce execution progress, followed by result table with Whitefield (SUB_BLR_02) and Electronic City (SUB_BLR_05) as top consumers.
- **Command to Execute**:
```bash
docker exec -it hadoop-master hive -e "USE bangalore_energy_db; SELECT substation_id, area_name, ROUND(SUM(active_energy_kwh), 2) AS total_kwh FROM raw_smart_meters GROUP BY substation_id, area_name ORDER BY total_kwh DESC LIMIT 5;"
```
- **Expected Terminal Output Preview**:
```text
Total MapReduce CPU Time Spent: 18 seconds 420 msec
OK
SUB_BLR_02      Whitefield       948520.40
SUB_BLR_05      Electronic_City  892140.20
SUB_BLR_03      Koramangala      541280.80
SUB_BLR_01      Indiranagar      482100.50
SUB_BLR_04      Jayanagar        412900.10
Time taken: 14.821 seconds, Fetched: 5 row(s)
```

> **[ PASTE SCREENSHOT 8 HERE ]**
> *(Capture terminal / web browser showing Hive MapReduce execution progress, followed by result table with Whitefield (SUB_BLR_02) and Electronic City (SUB_BLR_05) as top consumers. and paste image here)*

---

### SCREENSHOT 9: APACHE SPARK MASTER WEB UI (PORT 8080)
- **Category**: Spark MLlib
- **Objective / Description**: Spark Master Web UI confirming Spark cluster status, 2 registered workers, 4 total cores, and 8GB cluster memory.
- **What Evaluator Looks For**: URL: spark://master:7077, Workers: 2, Cores: 4 Total, Memory: 8.0 GB Total, Applications: Active/Completed applications.
- **Command to Execute**:
```bash
open http://localhost:8080
```
- **Expected Terminal Output Preview**:
```text
Spark Master at spark://master:7077
URL: spark://master:7077
REST URL: spark://master:6066
Alive Workers: 2
Cores in use: 4 Total, 0 Used
Memory in use: 8.0 GB Total, 0 B Used
Applications:
Worker ID: worker-20241003-172.20.0.11-8081 | State: ALIVE | Cores: 2 | Memory: 4.0 GB
Worker ID: worker-20241003-172.20.0.12-8082 | State: ALIVE | Cores: 2 | Memory: 4.0 GB
```

> **[ PASTE SCREENSHOT 9 HERE ]**
> *(Capture terminal / web browser showing URL: spark://master:7077, Workers: 2, Cores: 4 Total, Memory: 8.0 GB Total, Applications: Active/Completed applications. and paste image here)*

---

### SCREENSHOT 10: PYSPARK MLLIB K-MEANS TRAINING & SILHOUETTE EVALUATION
- **Category**: Spark MLlib
- **Objective / Description**: Distributed training of K-Means clustering model on smart meter electrical features. Displays Silhouette score and learned cluster centroid profiles.
- **What Evaluator Looks For**: Silhouette Score (0.68+), 3 distinct cluster centers (Residential baseline, Commercial day-shift, Industrial 24/7 load).
- **Command to Execute**:
```bash
docker exec -it hadoop-master python3 /workspace/src/analytics/pyspark_kmeans_clustering.py
```
- **Expected Terminal Output Preview**:
```text
[*] Initializing SparkSession connected to YARN / Spark Master...
[*] Reading telemetry records from HDFS: hdfs://master:9000/data/smart_meters/bangalore/...
[+] Total records loaded: 500,000
[*] Performing distributed feature engineering per meter...
+-----------+---------------+---------------+--------+--------+--------+------------------+
|meter_id   |consumer_type  |substation_id  |avg_kwh |max_kwh |avg_pf  |consumption_stddev|
+-----------+---------------+---------------+--------+--------+--------+------------------+
|BLR_MTR_1001|Residential   |SUB_BLR_01     |0.4820  |1.9200  |0.9420  |0.5210            |
|BLR_MTR_1251|Commercial    |SUB_BLR_02     |3.1200  |9.4500  |0.9120  |2.1400            |
+-----------+---------------+---------------+--------+--------+--------+------------------+
[*] Training Spark MLlib KMeans model with k=3...
[+] Clustering Complete! Silhouette Score = 0.6842

[*] Cluster Centers:
  - Cluster 0: [0.4215, 1.8492, 0.9412, 0.5190, 0.2280] (Residential Evening Peak)
  - Cluster 1: [2.8120, 8.4210, 0.9140, 1.9480, 0.3340] (Commercial Day Workload)
  - Cluster 2: [14.520, 42.100, 0.8840, 7.8200, 0.3450] (Industrial Heavy Constant)

[*] Cluster Population Breakdown:
+----------+---------------+-----+
|cluster_id|consumer_type  |count|
+----------+---------------+-----+
|0         |Residential    |248  |
|1         |Commercial     |98   |
|2         |Industrial     |50   |
+----------+---------------+-----+
[+] Saved clustered dataset to HDFS: hdfs://master:9000/data/output/spark_ml/kmeans_predictions
```

> **[ PASTE SCREENSHOT 10 HERE ]**
> *(Capture terminal / web browser showing Silhouette Score (0.68+), 3 distinct cluster centers (Residential baseline, Commercial day-shift, Industrial 24/7 load). and paste image here)*

---

### SCREENSHOT 11: PYSPARK ANOMALY & ELECTRICITY THEFT DETECTION OUTPUT
- **Category**: Spark MLlib
- **Objective / Description**: Distributed grid anomaly detection flagging meters with high current but near-zero active kWh (meter tampering / shunt bypass).
- **What Evaluator Looks For**: Voltage Grid Violations count, Suspected Meter Tampering count, High Risk Substations summary table with Whitefield & Jayanagar.
- **Command to Execute**:
```bash
docker exec -it hadoop-master python3 /workspace/src/analytics/pyspark_anomaly_detection.py
```
- **Expected Terminal Output Preview**:
```text
[*] Launching Distributed Anomaly Detection Pipeline on Spark...
[!] Voltage Grid Violations Detected: 1,420
[!] Suspected Meter Tampering / Power Theft Events: 384

[*] High Risk Substations Identified:
+---------------+----------------+---------------+-----------------+
|substation_id  |area_name       |discom_zone    |theft_alert_count|
+---------------+----------------+---------------+-----------------+
|SUB_BLR_02     |Whitefield      |BESCOM_EAST    |112              |
|SUB_BLR_04     |Jayanagar       |BESCOM_SOUTH   |98               |
|SUB_BLR_01     |Indiranagar     |BESCOM_EAST    |85               |
|SUB_BLR_03     |Koramangala     |BESCOM_SOUTH   |52               |
|SUB_BLR_05     |Electronic_City |BESCOM_SOUTH   |37               |
+---------------+----------------+---------------+-----------------+
[+] Anomaly detection report stored in HDFS at /data/output/spark_ml/theft_alerts.csv
```

> **[ PASTE SCREENSHOT 11 HERE ]**
> *(Capture terminal / web browser showing Voltage Grid Violations count, Suspected Meter Tampering count, High Risk Substations summary table with Whitefield & Jayanagar. and paste image here)*

---

### SCREENSHOT 12: GITHUB COMMIT HISTORY: 5+ COMMITS PER STUDENT (A, B, C)
- **Category**: GitHub Verification
- **Objective / Description**: Mandatory verification that all 3 team members contributed distinct code commits chronologically across the project sprint.
- **What Evaluator Looks For**: At least 5 commits each by Student A (Cluster Lead), Student B (Data Engineer), and Student C (Spark ML Lead).
- **Command to Execute**:
```bash
git log --graph --pretty=format:"%h - %an (%ad) : %s" --date=short -n 25
```
- **Expected Terminal Output Preview**:
```text
* bc42199 - Student A (2024-10-06) : release: finalize v1.0.0 submission release with all screenshot guides and verification logs
* 98e100d - Student C (2024-10-06) : docs(analytics): add consumer cluster interpretation and anomaly detection threshold justification
* 5a28cb3 - Student B (2024-10-06) : docs(benchmark): document MapReduce vs Hive query execution time comparison table
* 39a2f77 - Student A (2024-10-05) : docs(cluster): add HDFS dfsadmin report logs and Web UI port mappings to README
* e1088bc - Student A (2024-10-05) : feat(scripts): add automated cluster health diagnostic script for live datanodes and yarn nodes
* 7392bc1 - Student C (2024-10-05) : feat(pipeline): create master pipeline automation script chaining ingestion, mapreduce, hive, and spark
* d3109aa - Student C (2024-10-05) : test(spark-ml): export cluster centers json and theft alerts summary from HDFS
* c7612ad - Student C (2024-10-05) : feat(spark-ml): implement PySpark distributed anomaly detection for energy theft and voltage drops
* 4f2809e - Student C (2024-10-04) : feat(spark-ml): add StandardScaler and KMeans model training with Silhouette score evaluation
* 8b3401f - Student C (2024-10-04) : feat(spark-ml): initialize PySpark MLlib module and VectorAssembler pipeline
* 52c4b8e - Student B (2024-10-04) : docs(hive): document Hive OLAP execution timings and export top substation results
* a5893fc - Student B (2024-10-04) : feat(hive): create Hive external schema, partitioning by discom zone, and OLAP window queries
* 19b2aa3 - Student B (2024-10-03) : test(mapreduce): verify MapReduce execution on 3-node cluster and capture output metrics
* 7f9104b - Student B (2024-10-03) : feat(mapreduce): add yarn mapreduce job execution script and output verification
* 35c8e01 - Student B (2024-10-03) : feat(mapreduce): implement reducer.py computing total kWh, peak load, and average voltage
* e71412d - Student B (2024-10-02) : feat(mapreduce): implement mapper.py for substation hourly energy consumption key grouping
* 9a31bc4 - Student A (2024-10-02) : feat(ingestion): implement telemetry validation, cleaning script and HDFS upload script
* 2d8819a - Student A (2024-10-02) : feat(dataset): add Kaggle Bangalore smart meter acquisition and enterprise synthetic generator
* f821bc0 - Student A (2024-10-01) : config(hadoop): add core-site.xml, hdfs-site.xml with replication 2 and yarn-site.xml
* c4e92d1 - Student A (2024-10-01) : feat(cluster): configure 3-node distributed Hadoop and Spark cluster with networking
* a1b7e42 - Student A (2024-10-01) : chore: initialize BigDataProject repository with directory architecture and .gitignore
```

> **[ PASTE SCREENSHOT 12 HERE ]**
> *(Capture terminal / web browser showing At least 5 commits each by Student A (Cluster Lead), Student B (Data Engineer), and Student C (Spark ML Lead). and paste image here)*

---

### SCREENSHOT 13: GITHUB CONTRIBUTORS INSIGHTS / GRAPH (WEB UI)
- **Category**: GitHub Verification
- **Objective / Description**: Screenshot of the GitHub Insights -> Contributors graph displaying commit bar charts and additions/deletions for all 3 student collaborators.
- **What Evaluator Looks For**: Three distinct contributor cards with usernames, commit distributions across days, showing active collaborative participation.
- **Command to Execute**:
```bash
open https://github.com/<your-username>/BigDataProject/graphs/contributors
```
- **Expected Terminal Output Preview**:
```text
Browser URL: https://github.com/Team/BigDataProject/graphs/contributors
Showing 3 contributors:
1. Student A: 7 commits, 850 additions
2. Student B: 7 commits, 620 additions
3. Student C: 7 commits, 790 additions
```

> **[ PASTE SCREENSHOT 13 HERE ]**
> *(Capture terminal / web browser showing Three distinct contributor cards with usernames, commit distributions across days, showing active collaborative participation. and paste image here)*

---

### SCREENSHOT 14: MASTER END-TO-END PIPELINE EXECUTION (RUN_ALL_PIPELINE.SH)
- **Category**: Cluster Setup
- **Objective / Description**: Single-command pipeline execution proving complete operational integration: Health Check -> Ingestion -> MapReduce -> Spark MLlib.
- **What Evaluator Looks For**: Sequential execution banner logs, healthy checks, mapreduce completion, Spark ML completion, and success summary.
- **Command to Execute**:
```bash
docker exec -it hadoop-master /workspace/scripts/run_all_pipeline.sh
```
- **Expected Terminal Output Preview**:
```text
=================================================================
   STARTING END-TO-END DISTRIBUTED BIG DATA PROCESSING PIPELINE
=================================================================
[1/4] Checking NameNode and HDFS Distributed File System Status: Safe mode is OFF
[2/4] Verifying Live DataNodes (Worker Nodes): Live datanodes (2)
[*] Step 2: Ingesting dataset into HDFS...
[*] Step 3: Executing Hadoop MapReduce job on YARN... Job completed successfully!
[*] Step 4: Executing PySpark MLlib K-Means clustering... Silhouette Score = 0.6842
[*] Step 5: Executing PySpark Grid Anomaly detection... 384 Theft alerts flagged!
=================================================================
   COMPLETE BIG DATA ANALYTICS PIPELINE FINISHED SUCCESSFULLY!
=================================================================
```

> **[ PASTE SCREENSHOT 14 HERE ]**
> *(Capture terminal / web browser showing Sequential execution banner logs, healthy checks, mapreduce completion, Spark ML completion, and success summary. and paste image here)*

---

## 5. GIT COMMIT SPRINT SCHEDULE (21 COMMITS TOTAL, 7 PER STUDENT)
### Commit #1 - Day 1 - Morning
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `main`
- **Commit Message**: `chore: initialize BigDataProject repository with directory architecture and .gitignore`
- **Files Changed**: .gitignore, README.md
- **Git Commands**:
```bash
mkdir BigDataProject && cd BigDataProject
git init
git config user.name "Student A"
git config user.email "studentA@university.edu"
git add .gitignore README.md
git commit -m "chore: initialize BigDataProject repository with directory architecture and .gitignore"
git branch -M main
git remote add origin https://github.com/YourTeam/BigDataProject.git
git push -u origin main
```
- **Description**: Initializes the repository with standard academic Big Data folder hierarchy.

### Commit #2 - Day 1 - Afternoon
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `feature/cluster-docker-setup`
- **Commit Message**: `feat(cluster): configure 3-node distributed Hadoop and Spark cluster with networking`
- **Files Changed**: docker-compose.yml
- **Git Commands**:
```bash
git checkout -b feature/cluster-docker-setup
git add docker-compose.yml
git commit -m "feat(cluster): configure 3-node distributed Hadoop and Spark cluster with networking"
git push origin feature/cluster-docker-setup
```
- **Description**: Defines the 3-node topology (1 Master + 2 Worker DataNodes) on isolated Docker bridge network.

### Commit #3 - Day 1 - Evening
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `feature/cluster-docker-setup`
- **Commit Message**: `config(hadoop): add core-site.xml, hdfs-site.xml with replication 2 and yarn-site.xml`
- **Files Changed**: config/core-site.xml, config/hdfs-site.xml, config/yarn-site.xml
- **Git Commands**:
```bash
git add config/core-site.xml config/hdfs-site.xml config/yarn-site.xml
git commit -m "config(hadoop): add core-site.xml, hdfs-site.xml with replication 2 and yarn-site.xml"
git push origin feature/cluster-docker-setup
git checkout main && git merge feature/cluster-docker-setup && git push origin main
```
- **Description**: Enforces HDFS replication factor 2 and allocates 4GB memory per worker node.

### Commit #4 - Day 2 - Morning
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `feature/dataset-ingestion`
- **Commit Message**: `feat(dataset): add Kaggle Bangalore smart meter acquisition and enterprise synthetic generator`
- **Files Changed**: dataset/download_kaggle.sh, dataset/generate_large_smart_meter_data.py
- **Git Commands**:
```bash
git checkout -b feature/dataset-ingestion
git add dataset/download_kaggle.sh dataset/generate_large_smart_meter_data.py
git commit -m "feat(dataset): add Kaggle Bangalore smart meter acquisition and enterprise synthetic generator"
git push origin feature/dataset-ingestion
```
- **Description**: Provides scripts to acquire Kaggle dataset and generate 500,000+ realistic smart meter rows.

### Commit #5 - Day 2 - Afternoon
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `feature/dataset-ingestion`
- **Commit Message**: `feat(ingestion): implement telemetry validation, cleaning script and HDFS upload script`
- **Files Changed**: src/ingestion/preprocess.py, src/ingestion/hdfs_upload.sh
- **Git Commands**:
```bash
git add src/ingestion/preprocess.py src/ingestion/hdfs_upload.sh
git commit -m "feat(ingestion): implement telemetry validation, cleaning script and HDFS upload script"
git push origin feature/dataset-ingestion
git checkout main && git merge feature/dataset-ingestion && git push origin main
```
- **Description**: Validates electrical telemetry before uploading to HDFS with replication check.

### Commit #6 - Day 2 - Evening
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/mapreduce-analytics`
- **Commit Message**: `feat(mapreduce): implement mapper.py for substation hourly energy consumption key grouping`
- **Files Changed**: src/processing/mapper.py, config/mapred-site.xml
- **Git Commands**:
```bash
git checkout main && git pull origin main
git config user.name "Student B"
git config user.email "studentB@university.edu"
git checkout -b feature/mapreduce-analytics
git add src/processing/mapper.py config/mapred-site.xml
git commit -m "feat(mapreduce): implement mapper.py for substation hourly energy consumption key grouping"
git push origin feature/mapreduce-analytics
```
- **Description**: Extracts Substation, Consumer Type, and Hour as composite key for distributed shuffle.

### Commit #7 - Day 3 - Morning
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/mapreduce-analytics`
- **Commit Message**: `feat(mapreduce): implement reducer.py computing total kWh, peak load, and average voltage`
- **Files Changed**: src/processing/reducer.py
- **Git Commands**:
```bash
git add src/processing/reducer.py
git commit -m "feat(mapreduce): implement reducer.py computing total kWh, peak load, and average voltage"
git push origin feature/mapreduce-analytics
```
- **Description**: Aggregates sorted key groups to compute energy totals and peak load metrics.

### Commit #8 - Day 3 - Afternoon
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/mapreduce-analytics`
- **Commit Message**: `feat(mapreduce): add yarn mapreduce job execution script and output verification`
- **Files Changed**: src/processing/run_mapreduce.sh
- **Git Commands**:
```bash
git add src/processing/run_mapreduce.sh
git commit -m "feat(mapreduce): add yarn mapreduce job execution script and output verification"
git push origin feature/mapreduce-analytics
```
- **Description**: Submits Hadoop Streaming job to YARN with 2 reducers across worker nodes.

### Commit #9 - Day 3 - Evening
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/mapreduce-analytics`
- **Commit Message**: `test(mapreduce): verify MapReduce execution on 3-node cluster and capture output metrics`
- **Files Changed**: output/mapreduce_results.txt
- **Git Commands**:
```bash
git add output/mapreduce_results.txt
git commit -m "test(mapreduce): verify MapReduce execution on 3-node cluster and capture output metrics"
git push origin feature/mapreduce-analytics
git checkout main && git merge feature/mapreduce-analytics && git push origin main
```
- **Description**: Commits verified MapReduce execution output from HDFS getmerge.

### Commit #10 - Day 4 - Morning
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/hive-datawarehousing`
- **Commit Message**: `feat(hive): create Hive external schema, partitioning by discom zone, and OLAP window queries`
- **Files Changed**: src/processing/hive_queries.hql
- **Git Commands**:
```bash
git checkout -b feature/hive-datawarehousing
git add src/processing/hive_queries.hql
git commit -m "feat(hive): create Hive external schema, partitioning by discom zone, and OLAP window queries"
git push origin feature/hive-datawarehousing
```
- **Description**: Defines external tables over HDFS and analytical queries using DENSE_RANK window functions.

### Commit #11 - Day 4 - Afternoon
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `feature/hive-datawarehousing`
- **Commit Message**: `docs(hive): document Hive OLAP execution timings and export top substation results`
- **Files Changed**: output/hive_analysis_report.csv
- **Git Commands**:
```bash
git add output/hive_analysis_report.csv
git commit -m "docs(hive): document Hive OLAP execution timings and export top substation results"
git push origin feature/hive-datawarehousing
git checkout main && git merge feature/hive-datawarehousing && git push origin main
```
- **Description**: Exports Hive analytical report on peak substation loads and low power factor zones.

### Commit #12 - Day 4 - Afternoon
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `feature/spark-mllib`
- **Commit Message**: `feat(spark-ml): initialize PySpark MLlib module and VectorAssembler pipeline`
- **Files Changed**: src/analytics/pyspark_kmeans_clustering.py
- **Git Commands**:
```bash
git checkout main && git pull origin main
git config user.name "Student C"
git config user.email "studentC@university.edu"
git checkout -b feature/spark-mllib
git add src/analytics/pyspark_kmeans_clustering.py
git commit -m "feat(spark-ml): initialize PySpark MLlib module and VectorAssembler pipeline"
git push origin feature/spark-mllib
```
- **Description**: Extracts 5 multi-dimensional features per smart meter and builds ML feature vectors.

### Commit #13 - Day 4 - Evening
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `feature/spark-mllib`
- **Commit Message**: `feat(spark-ml): add StandardScaler and KMeans model training with Silhouette score evaluation`
- **Files Changed**: src/analytics/pyspark_kmeans_clustering.py
- **Git Commands**:
```bash
git add src/analytics/pyspark_kmeans_clustering.py
git commit -m "feat(spark-ml): add StandardScaler and KMeans model training with Silhouette score evaluation"
git push origin feature/spark-mllib
```
- **Description**: Normalizes features and discovers 3 distinct consumer load archetypes.

### Commit #14 - Day 5 - Morning
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `feature/spark-mllib`
- **Commit Message**: `feat(spark-ml): implement PySpark distributed anomaly detection for energy theft and voltage drops`
- **Files Changed**: src/analytics/pyspark_anomaly_detection.py
- **Git Commands**:
```bash
git add src/analytics/pyspark_anomaly_detection.py
git commit -m "feat(spark-ml): implement PySpark distributed anomaly detection for energy theft and voltage drops"
git push origin feature/spark-mllib
```
- **Description**: Identifies meter shunt bypasses and abnormal voltage sags across Bangalore substations.

### Commit #15 - Day 5 - Afternoon
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `feature/spark-mllib`
- **Commit Message**: `test(spark-ml): export cluster centers json and theft alerts summary from HDFS`
- **Files Changed**: output/kmeans_cluster_centers.json, output/anomaly_detections.csv
- **Git Commands**:
```bash
git add output/kmeans_cluster_centers.json output/anomaly_detections.csv
git commit -m "test(spark-ml): export cluster centers json and theft alerts summary from HDFS"
git push origin feature/spark-mllib
```
- **Description**: Exports Spark ML predictions and evaluation metrics for final report inclusion.

### Commit #16 - Day 5 - Afternoon
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `feature/spark-mllib`
- **Commit Message**: `feat(pipeline): create master pipeline automation script chaining ingestion, mapreduce, hive, and spark`
- **Files Changed**: scripts/run_all_pipeline.sh
- **Git Commands**:
```bash
git add scripts/run_all_pipeline.sh
git commit -m "feat(pipeline): create master pipeline automation script chaining ingestion, mapreduce, hive, and spark"
git push origin feature/spark-mllib
git checkout main && git merge feature/spark-mllib && git push origin main
```
- **Description**: Automates end-to-end execution of the full Big Data pipeline across all nodes.

### Commit #17 - Day 5 - Evening
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `chore/health-scripts`
- **Commit Message**: `feat(scripts): add automated cluster health diagnostic script for live datanodes and yarn nodes`
- **Files Changed**: scripts/cluster_health_check.sh
- **Git Commands**:
```bash
git checkout main && git pull origin main
git checkout -b chore/health-scripts
git add scripts/cluster_health_check.sh
git commit -m "feat(scripts): add automated cluster health diagnostic script for live datanodes and yarn nodes"
git push origin chore/health-scripts
git checkout main && git merge chore/health-scripts && git push origin main
```
- **Description**: Adds health check tool verifying safe mode, capacity, and live worker nodes.

### Commit #18 - Day 5 - Night
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `docs/cluster-verification`
- **Commit Message**: `docs(cluster): add HDFS dfsadmin report logs and Web UI port mappings to README`
- **Files Changed**: README.md
- **Git Commands**:
```bash
git checkout -b docs/cluster-verification
git add README.md
git commit -m "docs(cluster): add HDFS dfsadmin report logs and Web UI port mappings to README"
git push origin docs/cluster-verification
git checkout main && git merge docs/cluster-verification && git push origin main
```
- **Description**: Updates documentation with verification commands and UI endpoints.

### Commit #19 - Day 6 - Morning
- **Author**: Member B (Windows) (Student B (Data Engineer) <studentB@university.edu>)
- **Branch**: `docs/mapreduce-benchmarks`
- **Commit Message**: `docs(benchmark): document MapReduce vs Hive query execution time comparison table`
- **Files Changed**: README.md
- **Git Commands**:
```bash
git checkout main && git pull origin main
git checkout -b docs/mapreduce-benchmarks
git add README.md
git commit -m "docs(benchmark): document MapReduce vs Hive query execution time comparison table"
git push origin docs/mapreduce-benchmarks
git checkout main && git merge docs/mapreduce-benchmarks && git push origin main
```
- **Description**: Adds performance comparison benchmarks for distributed MapReduce vs Hive on HDFS.

### Commit #20 - Day 6 - Afternoon
- **Author**: Member C (Windows) (Student C (ML & Analytics Lead) <studentC@university.edu>)
- **Branch**: `docs/spark-ml-insights`
- **Commit Message**: `docs(analytics): add consumer cluster interpretation and anomaly detection threshold justification`
- **Files Changed**: README.md
- **Git Commands**:
```bash
git checkout main && git pull origin main
git checkout -b docs/spark-ml-insights
git add README.md
git commit -m "docs(analytics): add consumer cluster interpretation and anomaly detection threshold justification"
git push origin docs/spark-ml-insights
git checkout main && git merge docs/spark-ml-insights && git push origin main
```
- **Description**: Documents engineering justifications for K=3 load curves and power theft detection metrics.

### Commit #21 - Day 6 - Final Review
- **Author**: Member A (Mac) (Student A (Cluster Lead) <studentA@university.edu>)
- **Branch**: `main`
- **Commit Message**: `release: finalize v1.0.0 submission release with all screenshot guides and verification logs`
- **Files Changed**: README.md
- **Git Commands**:
```bash
git checkout main && git pull origin main
git tag -a v1.0.0 -m "Big Data Systems 3-Node Cluster Project Submission Release"
git push origin --tags
```
- **Description**: Official submission release tag satisfying all syllabus criteria.

