from pyspark.sql import SparkSession
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

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "postgres"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "postgres"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "warehouse"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)


def load_from_bucket():

    spark = (
        SparkSession.builder
        .appName("yellow-bronze-load")
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
        "s3a://lakehouse/bronze/yellow/2026-01.parquet"
    )

    return df, spark


def save_to_db(df):

    jdbc_url = (
        f"jdbc:postgresql://"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )

    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/warehouse")
        .option("dbtable", "yellow_trips")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":

    spark = None

    try:

        df, spark = load_from_bucket()

        save_to_db(df)

        print(
            "Data successfully loaded into PostgreSQL."
        )

    except Exception as e:

        print(
            f"Error during loading: {e}"
        )

        raise

    finally:

        if spark is not None:
            spark.stop()