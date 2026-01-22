from common import get_spark_session, get_logger, PATHS
from pyspark.sql.functions import col, trim, split, when, lit, substring, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def main():
    spark = get_spark_session("Bronze_To_Silver")
    logger = get_logger("02_Silver")
    
    logger.info("Step 2: Cleaning Data (Silver Layer)")

    # --- A. Berkeley Earth Cleaning ---
    src_berkeley = f"{PATHS['bronze']}/berkeley_earth"
    dest_berkeley = f"{PATHS['silver']}/berkeley_clean"

    try:
        raw_df = spark.read.parquet(src_berkeley)
        # Parse logic: Split by whitespace, ignore comments
        parsed_df = raw_df.filter(~col("value").startswith("%")) \
            .withColumn("parts", split(trim(col("value")), "\\s+")) \
            .select(
                col("parts")[1].cast(IntegerType()).alias("year"),
                col("parts")[2].cast(IntegerType()).alias("month"),
                col("parts")[3].cast(IntegerType()).alias("day"), 
                col("parts")[5].cast(FloatType()).alias("anomaly") 
            )
        
        # Validation
        valid_df = parsed_df.dropna(subset=["year", "month", "anomaly"])
        invalid_count = parsed_df.count() - valid_df.count()
        
        if invalid_count > 0:
            logger.warning(f"⚠ Dropped {invalid_count} invalid rows from Berkeley data")

        valid_df.write.mode("overwrite").parquet(dest_berkeley)
        logger.info(f"✔ Written {valid_df.count()} rows to {dest_berkeley}")

    except Exception as e:
        logger.error(f"Failed processing Berkeley: {e}")
        # Proceeding to allow partial pipeline success if possible, or exit?
        # Let's exit to be strict.
        sys.exit(1)

    # --- B. NOAA Stations Cleaning ---
    src_noaa = f"{PATHS['bronze']}/noaa_stations"
    dest_noaa = f"{PATHS['silver']}/stations_clean"

    try:
        raw_s_df = spark.read.parquet(src_noaa)
        
        # Fixed Width Parsing (IVD)
        # ID: 0-11, LAT: 12-20, LON: 21-30, ELEV: 31-37, STATE: 38-40, NAME: 41-71
        # substring index is 1-based, length
        # pyspark substring(str, pos, len)
        stations_df = raw_s_df.select(
            trim(substring("value", 1, 11)).alias("station_id"),
            trim(substring("value", 13, 8)).cast(FloatType()).alias("latitude"),
            trim(substring("value", 22, 9)).cast(FloatType()).alias("longitude"),
            trim(substring("value", 32, 6)).cast(FloatType()).alias("elevation"),
            trim(substring("value", 39, 2)).alias("state"),
            trim(substring("value", 42, 30)).alias("name")
        )

        valid_st = stations_df.dropna(subset=["station_id", "latitude", "longitude"])
        
        valid_st.write.mode("overwrite").parquet(dest_noaa)
        logger.info(f"✔ Written {valid_st.count()} stations to {dest_noaa}")

    except Exception as e:
        logger.error(f"Failed processing NOAA: {e}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    import sys
    main()
