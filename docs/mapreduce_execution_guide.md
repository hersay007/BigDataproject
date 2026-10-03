# MapReduce & YARN Distributed Execution Guide
**Author**: Udayan Bhargava (Member B - Data Processing & Hive Engineer)

## 1. Job Architecture & Topology
- **Framework**: Apache Hadoop MapReduce Streaming on YARN
- **Input Dataset**: `/data/smart_meters/bangalore/bangalore_smart_meters_clean.csv` (198.66 MB)
- **Input Splits**: 2 splits (calculated from 128MB HDFS block boundary)
- **Mapper Execution**: `/usr/bin/python3 mapper.py`
  - Parses streaming telemetry intervals from standard input.
  - Groups composite keys: `substation_id#consumer_type#hour`.
  - Filters out headers and malformed voltage/current rows.
- **Reducer Execution**: `/usr/bin/python3 reducer.py`
  - Number of Reducers: 2 (distributed across worker1 and worker2).
  - Computes cumulative active energy (kWh), peak line current (A), and mean voltage (V).

## 2. Cluster Resource Allocation
- **Container Memory**: 1024 MB
- **Java Heap Size**: 768 MB (`-Xmx768m`)
- **Job Status**: SUCCESS (Map: 100%, Reduce: 100%)
- **HDFS Output**: `/data/output/mapreduce/peak_demand_summary/`