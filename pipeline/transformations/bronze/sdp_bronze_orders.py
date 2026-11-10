from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set in the pipeline configuration (see databricks.yml)
LANDING = spark.conf.get("landing_path")


# Bronze: raw orders ingested incrementally with Auto Loader.
# Schema location + checkpoints are managed by the pipeline automatically.
@dp.table(comment="Raw orders JSON ingested incrementally with Auto Loader")
def sdp_bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_ts TIMESTAMP, customer_id BIGINT")
        .load(f"{LANDING}/orders")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )
