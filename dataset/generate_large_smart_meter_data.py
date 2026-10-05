#!/usr/bin/env python3
"""
Bangalore Smart Energy Meters - Enterprise Big Data Generator
Mirrors schema of Kaggle: unseemlycoder/smart-energy-meters-in-bangalore-india
Simulates 15-minute telemetry intervals, real diurnal load curves, power factor variations,
and deliberate tampering/anomaly injections for Spark MLlib evaluation.
"""

import csv
import argparse
import random
import sys
from datetime import datetime, timedelta

SUBSTATIONS = [
    ("SUB_BLR_01", "Indiranagar", "BESCOM_EAST"),
    ("SUB_BLR_02", "Whitefield", "BESCOM_EAST"),
    ("SUB_BLR_03", "Koramangala", "BESCOM_SOUTH"),
    ("SUB_BLR_04", "Jayanagar", "BESCOM_SOUTH"),
    ("SUB_BLR_05", "Electronic_City", "BESCOM_SOUTH"),
    ("SUB_BLR_06", "Malleshwaram", "BESCOM_NORTH"),
    ("SUB_BLR_07", "Hebbal", "BESCOM_NORTH"),
    ("SUB_BLR_08", "Rajajinagar", "BESCOM_WEST")
]

CONSUMER_PROFILES = {
    "Residential": {"count": 250, "base_kw": 1.2, "peak_kw": 4.5, "peak_hours": [6, 7, 8, 19, 20, 21, 22]},
    "Commercial": {"count": 100, "base_kw": 5.0, "peak_kw": 25.0, "peak_hours": [9, 10, 11, 12, 14, 15, 16, 17, 18]},
    "Industrial": {"count": 50, "base_kw": 30.0, "peak_kw": 120.0, "peak_hours": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]}
}

def generate_dataset(num_records: int, output_file: str):
    print(f"[*] Generating {num_records:,} smart meter records for Bangalore power grid...")
    
    # Initialize meters
    meters = []
    meter_idx = 1001
    for ctype, profile in CONSUMER_PROFILES.items():
        for _ in range(profile["count"]):
            sub = random.choice(SUBSTATIONS)
            meters.append({
                "meter_id": f"BLR_MTR_{meter_idx}",
                "consumer_type": ctype,
                "substation_id": sub[0],
                "area_name": sub[1],
                "discom_zone": sub[2],
                "base_kw": profile["base_kw"],
                "peak_kw": profile["peak_kw"],
                "peak_hours": set(profile["peak_hours"]),
                # Inject 3% of meters with recurring theft/tampering behavior
                "is_anomalous": (random.random() < 0.03)
            })
            meter_idx += 1

    start_date = datetime(2024, 1, 1, 0, 0, 0)
    current_time = start_date

    fieldnames = [
        "meter_id", "timestamp", "consumer_type", "substation_id", "area_name", "discom_zone",
        "active_energy_kwh", "reactive_energy_kvarh", "voltage_v", "current_a",
        "power_factor", "frequency_hz", "anomaly_flag"
    ]

    records_written = 0
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while records_written < num_records:
            current_hour = current_time.hour
            time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")

            for m in meters:
                if records_written >= num_records:
                    break

                is_peak = current_hour in m["peak_hours"]
                # Diurnal load modeling
                if is_peak:
                    kw = random.uniform(m["base_kw"] * 1.5, m["peak_kw"])
                else:
                    kw = random.uniform(m["base_kw"] * 0.4, m["base_kw"] * 1.2)

                # Anomaly injection: meter bypass (near zero consumption during peak) or extreme surge
                anomaly_flag = 0
                if m["is_anomalous"] and random.random() < 0.25:
                    if random.random() < 0.6:
                        # Energy Theft / Shunt Bypass: near zero usage despite active hours
                        kw = kw * 0.02
                        anomaly_flag = 1
                    else:
                        # Fault / Phase Overload surge
                        kw = kw * 3.5
                        anomaly_flag = 2

                # 15-minute interval kWh = kW * 0.25
                active_kwh = round(kw * 0.25, 4)
                pf = round(random.uniform(0.85, 0.98), 3) if anomaly_flag == 0 else round(random.uniform(0.50, 0.75), 3)
                reactive_kvarh = round(active_kwh * ((1 - pf**2)**0.5 / pf), 4)

                # Grid voltage nominal 230V single phase / 400V 3-phase
                voltage = round(random.gauss(230.0, 3.5) if m["consumer_type"] == "Residential" else random.gauss(400.0, 5.0), 2)
                current = round((kw * 1000) / (voltage * pf), 2)
                freq = round(random.gauss(50.0, 0.08), 2)

                writer.writerow({
                    "meter_id": m["meter_id"],
                    "timestamp": time_str,
                    "consumer_type": m["consumer_type"],
                    "substation_id": m["substation_id"],
                    "area_name": m["area_name"],
                    "discom_zone": m["discom_zone"],
                    "active_energy_kwh": active_kwh,
                    "reactive_energy_kvarh": reactive_kvarh,
                    "voltage_v": voltage,
                    "current_a": current,
                    "power_factor": pf,
                    "frequency_hz": freq,
                    "anomaly_flag": anomaly_flag
                })
                records_written += 1

            current_time += timedelta(minutes=15)

    print(f"[+] Complete! Generated {records_written:,} records -> {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Smart Meter Data")
    parser.add_argument("--records", type=int, default=100000, help="Number of records to produce")
    parser.add_argument("--output", type=str, default="bangalore_smart_meters.csv", help="Output CSV path")
    args = parser.parse_args()
    generate_dataset(args.records, args.output)
