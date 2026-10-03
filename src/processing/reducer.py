#!/usr/bin/env python3
"""
Hadoop Streaming Reducer for Bangalore Smart Grid Peak Load Analytics.
Aggregates total energy consumption, peak current, and average grid voltage.
"""
import sys

current_key = None
total_kwh = 0.0
max_current = 0.0
voltage_sum = 0.0
total_readings = 0

print("substation_id\tconsumer_type\thour\ttotal_kwh\tmax_current_a\tavg_voltage_v\treading_count")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, val_str = line.split("\t", 1)
        kwh, current, voltage, count = val_str.split(",")
        kwh = float(kwh)
        current = float(current)
        voltage = float(voltage)
        count = int(count)
    except (ValueError, IndexError):
        continue

    if current_key == key:
        total_kwh += kwh
        max_current = max(max_current, current)
        voltage_sum += voltage
        total_readings += count
    else:
        if current_key:
            parts = current_key.split("#")
            sub_id = parts[0]
            ctype = parts[1] if len(parts) > 1 else "Unknown"
            hour = parts[2] if len(parts) > 2 else "00"
            avg_voltage = round(voltage_sum / total_readings, 2) if total_readings > 0 else 0.0
            print(f"{sub_id}\t{ctype}\t{hour}\t{total_kwh:.2f}\t{max_current:.2f}\t{avg_voltage}\t{total_readings}")

        current_key = key
        total_kwh = kwh
        max_current = current
        voltage_sum = voltage
        total_readings = count

if current_key and total_readings > 0:
    parts = current_key.split("#")
    sub_id = parts[0]
    ctype = parts[1] if len(parts) > 1 else "Unknown"
    hour = parts[2] if len(parts) > 2 else "00"
    avg_voltage = round(voltage_sum / total_readings, 2) if total_readings > 0 else 0.0
    print(f"{sub_id}\t{ctype}\t{hour}\t{total_kwh:.2f}\t{max_current:.2f}\t{avg_voltage}\t{total_readings}")