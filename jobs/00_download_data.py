import os
import requests
import random
import csv
from datetime import datetime, timedelta

try:
    from common import PATHS
    DATA_DIR = PATHS["raw"]
except ImportError:
    # Fallback if run from host without setting pythonpath
    DATA_DIR = "data/raw"
URLS = {
    "Global_Temperatures.txt": "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Complete_TAVG_daily.txt",
    "ghcnd-stations.txt": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
}

def download_file(url, dest):
    print(f"DTO Downloading {url}...")
    try:
        r = requests.get(url, stream=True, timeout=10)
        r.raise_for_status()
        with open(dest, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✔ Success: {dest}")
        return True
    except Exception as e:
        print(f"✘ Failed: {e}")
        return False

def generate_sample_berkeley(path):
    print("⚠ Generating synthetic Berkeley Earth data...")
    with open(path, 'w') as f:
        f.write("% Year, Month, Day, DecYear, Anomaly, ...\n")
        start_date = datetime(2000, 1, 1)
        for i in range(365 * 5): # 5 years
            curr = start_date + timedelta(days=i)
            # Random anomaly between -2 and +2
            anom = random.uniform(-2, 2)
            f.write(f"  {curr.year}  {curr.month:02d}  {curr.day:02d}  {curr.year}.00  {anom:.3f}\n")
    print(f"✔ Generated: {path}")

def generate_sample_stations(path):
    print("⚠ Generating synthetic NOAA Stations data...")
    # Fixed width format simulation
    # ID (11), LAT (8), LON (9), ELEV (6), STATE (2), NAME (30), ...
    with open(path, 'w') as f:
        for i in range(10):
            sid = f"USW000{i:05d}"
            lat = f"{random.uniform(25.0, 50.0):8.4f}"
            lon = f"{random.uniform(-125.0, -65.0):8.4f}"
            elev = "   100"
            state = "NY"
            name = f"TEST_STATION_{i}".ljust(30)
            line = f"{sid} {lat} {lon} {elev} {state} {name}\n"
            f.write(line)
    print(f"✔ Generated: {path}")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Berkeley
    p_berkeley = os.path.join(DATA_DIR, "Global_Temperatures.txt")
    if not os.path.exists(p_berkeley):
        if not download_file(URLS["Global_Temperatures.txt"], p_berkeley):
            generate_sample_berkeley(p_berkeley)
    
    # Stations
    p_stations = os.path.join(DATA_DIR, "ghcnd-stations.txt")
    if not os.path.exists(p_stations):
        if not download_file(URLS["ghcnd-stations.txt"], p_stations):
            generate_sample_stations(p_stations)

if __name__ == "__main__":
    main()
