from pyspark.sql import SparkSession
import logging
import sys

# Constants
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME_PREFIX = "Climate_Anomaly_"

# Paths (Container Views)
PATH_LOCAL_DATA = "/opt/spark/data"
PATH_HDFS_ROOT = "hdfs://namenode:9000/climate"

PATHS = {
    "bronze": f"{PATH_HDFS_ROOT}/bronze",
    "silver": f"{PATH_HDFS_ROOT}/silver",
    "gold":   f"{PATH_HDFS_ROOT}/gold",
    "raw":    f"{PATH_LOCAL_DATA}/raw",
    "power_bi": f"file://{PATH_LOCAL_DATA}/Power bi" # Local export view
}

def get_spark_session(job_name):
    """
    Creates a standardized SparkSession.
    """
    spark = SparkSession.builder \
        .appName(f"{APP_NAME_PREFIX}{job_name}") \
        .master(SPARK_MASTER) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_logger(name):
    """
    Returns a configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
