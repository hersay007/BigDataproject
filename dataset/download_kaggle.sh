#!/bin/bash
# =============================================================================
# Download Script for Bangalore Smart Energy Meters Dataset
# Dataset: unseemlycoder/smart-energy-meters-in-bangalore-india
# =============================================================================

set -e

DATASET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DATASET_DIR"

echo "================================================================="
echo "   BANGALORE SMART METERS - BIG DATASET ACQUISITION SCRIPT"
echo "================================================================="

if ! command -v kaggle &> /dev/null; then
    echo "[!] kaggle CLI not found. Installing via pip..."
    pip install --user kaggle || pip3 install kaggle
fi

# Check for ~/.kaggle/kaggle.json
if [ ! -f "$HOME/.kaggle/kaggle.json" ]; then
    echo "[!] Notice: $HOME/.kaggle/kaggle.json not found."
    echo "    To download directly via Kaggle API:"
    echo "    1. Go to https://www.kaggle.com/settings -> 'Create New Token'"
    echo "    2. Place kaggle.json in ~/.kaggle/ and chmod 600 ~/.kaggle/kaggle.json"
    echo "    Alternately, running the synthetic big data generator to generate"
    echo "    100,000 to 1,000,000+ realistic Bangalore smart meter telemetry records..."
    python3 generate_large_smart_meter_data.py --records 500000 --output bangalore_smart_meters.csv
    exit 0
fi

echo "[*] Downloading dataset from Kaggle..."
kaggle datasets download -d unseemlycoder/smart-energy-meters-in-bangalore-india -p "$DATASET_DIR" --unzip

echo "[+] Successfully downloaded and unzipped Bangalore smart meter dataset!"
ls -lh "$DATASET_DIR"/*.csv
