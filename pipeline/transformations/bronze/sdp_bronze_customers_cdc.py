from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


# Bronze: raw customer CDC feed (INSERT / UPDATE / DELETE events)
@dp.table(comment="Raw customer change events ingested with Auto Loader")
def sdp_bronze_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "customer_id BIGINT, updated_at TIMESTAMP")
        .load(f"{LANDING}/customers_cdc")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
