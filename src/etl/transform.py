from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


MINIO_PASSWORD = os.getenv(
    "MINIO_ROOT_PASSWORD",
    "minio1234"
)

MINIO_USER = os.getenv(
    "MINIO_ROOT_USER",
    "minio12345"
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT_S3",
    "http://minio:9000"
)


def load_from_bucket():

    spark = (
        SparkSession.builder
        .appName("yellow-bronze-transformation")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_USER
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_PASSWORD
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )

    df = spark.read.parquet(
        "s3a://lakehouse/raw/yellow/2026-01.parquet"
    )

    return df, spark


def transform_date(df):

    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit("2026-01-01")))
        & (F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit("2026-02-01")))
    )

    df = (
        df
        .withColumn(
            "pickup_year",
            F.year("tpep_pickup_datetime")
        )
        .withColumn(
            "pickup_month",
            F.month("tpep_pickup_datetime")
        )
        .withColumn(
            "pickup_day",
            F.dayofmonth("tpep_pickup_datetime")
        )
    )

    return df


def add_data_quality_flags(df):

    df = (
        df
        .withColumn(
            "has_null_passenger",
            F.when(
                F.col("passenger_count").isNull(),
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_invalid_distance",
            F.when(
                F.col("trip_distance") <= 0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_extreme_distance",
            F.when(
                F.col("trip_distance") > 100,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_negative_fare",
            F.when(
                F.col("fare_amount") < 0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_negative_total",
            F.when(
                F.col("total_amount") < 0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_negative_tip",
            F.when(
                F.col("tip_amount") < 0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_negative_tolls",
            F.when(
                F.col("tolls_amount") < 0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "has_invalid_dates",
            F.when(
                F.col("tpep_dropoff_datetime")
                < F.col("tpep_pickup_datetime"),
                1
            ).otherwise(0)
        )
    )

    return df


def save_processed_data(df):

    df.write \
        .mode("overwrite") \
        .parquet(
            "s3a://lakehouse/bronze/yellow/2026-01.parquet"
        )


if __name__ == "__main__":

    spark = None

    try:

        df, spark = load_from_bucket()

        df = transform_date(df)

        df = add_data_quality_flags(df)

        save_processed_data(df)

        print(
            "Transformation completed successfully."
        )

    except Exception as e:

        print(
            f"Error during transformation: {e}"
        )

        raise

    finally:

        if spark is not None:
            spark.stop()