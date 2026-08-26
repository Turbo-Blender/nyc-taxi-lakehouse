from pyspark.sql import SparkSession
from pathlib import Path
import os

MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

def extract_and_save():
    spark = (
    SparkSession.builder
    .appName("bronze-raw-ingestion")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
    )
    try:
        df = spark.read.parquet(
        "/app/data/raw/yellow/yellow_tripdata_2026-01.parquet"
        )

        df.write.mode("overwrite").parquet(
        "s3a://lakehouse/raw/yellow/2026-01.parquet"
        )
    except Exception as e:

        print(f"Error during extraction: {e}")

        raise

    finally:

        spark.stop()
    
if __name__ == "__main__":

    extract_and_save()
