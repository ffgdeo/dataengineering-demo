from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


# Bronze: clickstream events
@dp.table(comment="Raw clickstream events ingested with Auto Loader")
@dp.expect("has_event_time", "event_time IS NOT NULL")
def sdp_bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "event_time TIMESTAMP, user_id BIGINT")
        .load(f"{LANDING}/events")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
