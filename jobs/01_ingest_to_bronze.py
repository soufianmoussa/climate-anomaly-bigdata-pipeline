from common import get_spark_session, get_logger, PATHS
from pyspark.sql.functions import current_timestamp, lit
import os
import sys

def main():
    spark = get_spark_session("Ingest_Bronze")
    logger = get_logger("01_Ingest")
    
    logger.info("Step 1: Ingesting to Bronze Layer")
    
    # 1. Berkeley Earth
    src_berkeley = os.path.join(PATHS["raw"], "Global_Temperatures.txt")
    dest_berkeley = f"{PATHS['bronze']}/berkeley_earth"
    
    try:
        logger.info(f"Reading {src_berkeley}")
        df = spark.read.text(src_berkeley)
        df_meta = df.withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("source", lit("Berkeley_Earth"))
        
        df_meta.write.mode("overwrite").parquet(dest_berkeley)
        logger.info(f"✔ Written to {dest_berkeley}")
    except Exception as e:
        logger.error(f"Failed to ingest Berkeley data: {e}")
        sys.exit(1)

    # 2. NOAA Stations
    src_noaa = os.path.join(PATHS["raw"], "ghcnd-stations.txt")
    dest_noaa = f"{PATHS['bronze']}/noaa_stations"
    
    try:
        logger.info(f"Reading {src_noaa}")
        df_s = spark.read.text(src_noaa)
        df_s_meta = df_s.withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("source", lit("NOAA_Stations"))
        
        df_s_meta.write.mode("overwrite").parquet(dest_noaa)
        logger.info(f"✔ Written to {dest_noaa}")
    except Exception as e:
        logger.error(f"Failed to ingest NOAA data: {e}")
        # Non-critical? Academic requirement says "load raw data". 
        # For strictness we fail.
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
