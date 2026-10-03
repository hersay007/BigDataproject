#!/usr/bin/env python3
"""
Hadoop Streaming Mapper for Bangalore Smart Grid Peak Load Analytics.
Emits (substation_id#consumer_type#hour, active_energy_kwh, current_a, voltage_v, count).
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("meter_id"):
        continue

    parts = line.split(",")
    if len(parts) < 11:
        continue

    try:
        timestamp = parts[1].strip()
        consumer_type = parts[2].strip()
        substation_id = parts[3].strip()
        active_kwh = float(parts[6].strip())
        voltage = float(parts[8].strip())
        current = float(parts[9].strip())

        hour = "00"
        if " " in timestamp:
            time_part = timestamp.split(" ")[1]
            hour = time_part.split(":")[0]
        elif "T" in timestamp:
            time_part = timestamp.split("T")[1]
            hour = time_part.split(":")[0]

        composite_key = f"{substation_id}#{consumer_type}#{hour}"
        print(f"{composite_key}\t{active_kwh},{current},{voltage},1")
    except (ValueError, IndexError):
        continue