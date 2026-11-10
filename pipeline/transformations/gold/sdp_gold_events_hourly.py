from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Gold: hourly funnel metrics from clickstream
@dp.materialized_view(comment="Hourly event counts and unique users by event type and device")
def sdp_gold_events_hourly():
    return (
        spark.read.table("sdp_bronze_events")
        .withColumn("event_hour", F.date_trunc("hour", "event_time"))
        .groupBy("event_hour", "event_type", "device")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("user_id").alias("unique_users"),
        )
    )
