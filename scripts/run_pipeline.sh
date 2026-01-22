#!/bin/bash
set -e

echo "==================================================="
echo "   CLIMATE ANOMALY ANALYSIS - BATCH PIPELINE      "
echo "==================================================="

SPARK_CONTAINER="spark-master"
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
MASTER_URL="spark://spark-master:7077"

function run_job() {
    JOB_NAME=$1
    SCRIPT_PATH=$2
    echo "---------------------------------------------------"
    echo "DTO Running Step: $JOB_NAME"
    echo "---------------------------------------------------"
    docker exec $SPARK_CONTAINER $SPARK_SUBMIT \
        --master $MASTER_URL \
        --name "$JOB_NAME" \
        $SCRIPT_PATH
    echo "✔ Step Defined Complete."
}

# 1. Ingest (Bronze)
run_job "01_Ingest_To_Bronze" "/opt/spark/jobs/01_ingest_to_bronze.py"

# 2. Clean (Silver)
run_job "02_Bronze_To_Silver" "/opt/spark/jobs/02_bronze_to_silver.py"

# 3. Aggregate (Gold)
run_job "03_Silver_To_Gold" "/opt/spark/jobs/03_silver_to_gold.py"

echo "==================================================="
echo "   ✅ PIPELINE FINISHED SUCCESSFULLY               "
echo "==================================================="
