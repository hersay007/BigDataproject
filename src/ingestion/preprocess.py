#!/usr/bin/env python3
"""
Pre-processing and Transformation Script for Kaggle Bangalore Smart Meters
Parses raw InfluxDB time-series exports (e.g. SME-divya, SME-akash, SME-siva)
and pivots electrical metrics (voltage, current, power factor, active power)
into a structured tabular CSV optimized for HDFS distributed processing.
"""

import sys
import os
import csv
import glob

def process_influx_smart_meter(file_path: str, max_rows: int = 1000000):
    print(f"[*] Reading and transforming raw InfluxDB export: {file_path}")
    base_name = os.path.basename(file_path)
    
    # Infer meter metadata from filename
    if "divya" in base_name.lower():
        meter_id = "BLR_SME_DIVYA"
        consumer_type = "Commercial"
        substation_id = "SUB_BLR_02"
        area_name = "Whitefield"
        zone = "BESCOM_EAST"
    elif "akash" in base_name.lower():
        meter_id = "BLR_SME_AKASH"
        consumer_type = "Industrial"
        substation_id = "SUB_BLR_05"
        area_name = "Electronic_City"
        zone = "BESCOM_SOUTH"
    else:
        meter_id = "BLR_SME_SIVA"
        consumer_type = "Residential"
        substation_id = "SUB_BLR_01"
        area_name = "Indiranagar"
        zone = "BESCOM_EAST"

    records = []
    rows_read = 0

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 7:
                continue
            # Skip influx comment headers (#group, #datatype, #default)
            if row[0].startswith("#") or "result" in row[0] or "_time" in row:
                continue

            rows_read += 1
            if rows_read > max_rows:
                break

            try:
                # Influx schema: [, result, table, _start, _stop, _time, _value, _field, _measurement]
                time_str = row[5].replace("T", " ").replace("Z", "").split(".")[0] if len(row) > 5 else "2021-04-06 16:00:00"
                val = float(row[6]) if len(row) > 6 and row[6].strip() else 0.0
                field = row[7].strip().lower() if len(row) > 7 else "pf"

                records.append({
                    "timestamp": time_str,
                    "field": field,
                    "value": val
                })
            except (ValueError, IndexError):
                continue

    print(f"[+] Extracted {len(records):,} electrical metric measurements from {base_name}")
    return meter_id, consumer_type, substation_id, area_name, zone, records

def main():
    dataset_dir = "dataset"
    output_file = "dataset/bangalore_smart_meters_clean.csv"

    # Search for all CSV files in dataset/
    csv_files = glob.glob(os.path.join(dataset_dir, "SME*.csv"))
    if not csv_files:
        csv_files = [f for f in glob.glob(os.path.join(dataset_dir, "*.csv")) if "clean" not in f]

    if not csv_files:
        print("[!] No CSV files found in dataset/ folder.")
        sys.exit(1)

    print(f"[*] Found {len(csv_files)} smart meter datasets: {[os.path.basename(f) for f in csv_files]}")

    fieldnames = [
        "meter_id", "timestamp", "consumer_type", "substation_id", "area_name", "discom_zone",
        "active_energy_kwh", "reactive_energy_kvarh", "voltage_v", "current_a",
        "power_factor", "frequency_hz", "anomaly_flag"
    ]

    total_clean_records = 0
    with open(output_file, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        # Process each large file (taking up to 600,000 interval entries per meter for massive multi-million HDFS blocks)
        for fpath in csv_files:
            meter_id, ctype, sub_id, area, zone, raw_entries = process_influx_smart_meter(fpath, max_rows=600000)
            
            cur_voltage = 230.0 if ctype == "Residential" else 400.0
            cur_pf = 0.92
            cur_current = 8.5
            cur_kwh = 1.2

            for entry in raw_entries:
                f_name = entry["field"]
                val = entry["value"]

                if "pf" in f_name or "powerfactor" in f_name:
                    cur_pf = round(max(0.2, min(1.0, val)), 3)
                elif "v" in f_name or "volt" in f_name:
                    cur_voltage = round(val, 2) if val > 100 else cur_voltage
                elif "i" in f_name or "curr" in f_name:
                    cur_current = round(abs(val), 2)
                elif "p" in f_name or "power" in f_name or "energy" in f_name:
                    cur_kwh = round(abs(val) * 0.25, 4)

                # Anomaly detection flag for shunt bypass or severe voltage sag
                anomaly_flag = 0
                if cur_current > 15.0 and cur_kwh < 0.05:
                    anomaly_flag = 1
                elif cur_voltage < 190 or cur_voltage > 450:
                    anomaly_flag = 2

                reactive_kvarh = round(cur_kwh * ((1 - cur_pf**2)**0.5 / (cur_pf + 0.001)), 4)

                writer.writerow({
                    "meter_id": meter_id,
                    "timestamp": entry["timestamp"],
                    "consumer_type": ctype,
                    "substation_id": sub_id,
                    "area_name": area,
                    "discom_zone": zone,
                    "active_energy_kwh": cur_kwh,
                    "reactive_energy_kvarh": reactive_kvarh,
                    "voltage_v": cur_voltage,
                    "current_a": cur_current,
                    "power_factor": cur_pf,
                    "frequency_hz": 50.0,
                    "anomaly_flag": anomaly_flag
                })
                total_clean_records += 1

    print(f"\n[+] SUCCESS! Produced {total_clean_records:,} clean rows in {output_file}")
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"[+] Output File Size: {file_size_mb:.2f} MB (Ready for multi-block HDFS distribution)")

if __name__ == "__main__":
    main()
