from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
import os
schema = StructType([
    StructField("LocationID", IntegerType(), False),
    StructField("Borough", StringType(), False),
    StructField("Zone", StringType(), False),
    StructField("service_zone", StringType(), True),
])
spark = SparkSession.builder.appName("load-taxi-zones").getOrCreate()
zones = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("/app/data/nyc-zone-map/taxi_zone_lookup.csv")
    .selectExpr(
        "LocationID AS location_id",
        "Borough AS borough",
        "Zone AS zone_name",
        "service_zone",
    )
)
jdbc_url = (
    f"jdbc:postgresql://postgres:5432/"
    f"{os.getenv('POSTGRES_DB', 'warehouse')}"
)
(
    zones.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "reference.taxi_zones")
    .option("user", os.getenv("POSTGRES_USER", "postgres"))
    .option("password", os.getenv("POSTGRES_PASSWORD", "postgres"))
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
)
spark.stop()