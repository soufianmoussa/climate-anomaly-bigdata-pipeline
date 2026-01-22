# Analyse des Anomalies Climatiques (Big Data Project)

## Overview
This project implements a scalable Big Data pipeline to analyze global climate anomalies (temperature) using the **Medallion Architecture** (Bronze, Silver, Gold). The system handles data ingestion, cleaning, transformation, and aggregation to produce actionable insights for Power BI.

**Key Technologies:**
- **Docker & Docker Compose**: Containerized environment.
- **Apache Spark (3.5.0)**: Distributed data processing (Standalone Cluster).
- **HDFS (Hadoop 3.2.1)**: Distributed storage layer.
- **Python**: Spark application logic.

## Project Structure
```
.
├── config/                 # (Removed - Configs handled in jobs/common.py)
├── data/                   # Volume mapped to Spark containers
│   ├── raw/                # Input data (Downloaded automatically)
│   └── Power bi/           # FINAL OUTPUTS (CSVs)
├── jobs/                   # Spark Applications (Python)
│   ├── 00_download_data.py # Data acquisition
│   ├── 01_ingest_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_silver_to_gold.py
│   └── common.py           # Shared utilities
├── scripts/                # Helper scripts
│   ├── run_pipeline.sh     # Master execution script (Linux/WSL)
│   └── reset_env.sh        # Reset script
├── docker-compose.yml      # Service definitions
└── README.md
```

## Prerequisites
- Docker Engine & Docker Compose installed.
- Windows PowerShell (or Bash on Linux/Mac).
- 4GB+ RAM available for Docker.

## Quick Start

### 1. Start the Environment
Run the following from the project root:
```powershell
docker compose up -d
```
*Wait ~30 seconds for the Namenode and Spark Master to initialize.*

### 2. Run the Pipeline (One Command)

**Option A: Using PowerShell (Recommended for Windows)**
Copy and paste this block into your PowerShell terminal:
```powershell
# Step 0: Download Data
Write-Host "Downloading Data..."
docker exec spark-master python /opt/spark/jobs/00_download_data.py

# Step 1: Ingest (Bronze)
Write-Host "Running Bronze Layer..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/01_ingest_to_bronze.py

# Step 2: Clean (Silver)
Write-Host "Running Silver Layer..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/02_bronze_to_silver.py

# Step 3: Aggregate (Gold)
Write-Host "Running Gold Layer..."
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/03_silver_to_gold.py
```

**Option B: Using Bash (Linux/Mac/WSL)**
```bash
bash scripts/run_pipeline.sh
```

## Outputs
After specific successful execution, the final CSV files for Power BI are available in:
**`data/Power bi/`**

| File | Description |
|------|-------------|
| `climate_kpis.csv` | Global temperature trends and summary stats. |
| `stations_dim.csv` | Cleaned metadata for weather stations. |
| `climate_anomalies_monthly.csv` | Monthly temperature anomalies per station. |
| `climate_extremes.csv` | Detected extreme weather events (Heat/Cold waves). |

## Architecture Details
1.  **Bronze Layer**: Raw data (TXT/CSV) converted to Parquet on HDFS (`/climate/bronze`).
2.  **Silver Layer**: Cartesian join of temperatures and stations, schema enforcement, cleaning (`/climate/silver`).
3.  **Gold Layer**: Aggregations (Z-Score calculation), anomaly detection, and formatted export (`/climate/gold` & CSV).

## Resetting
To stop and clean everything (volumes included):
```powershell
docker compose down -v
```
