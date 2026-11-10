from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Gold: revenue by current customer tier and country (joins SCD1 dimension)
@dp.materialized_view(comment="Revenue by current customer tier and country")
def sdp_gold_revenue_by_tier():
    orders = spark.read.table("sdp_silver_orders")
    customers = spark.read.table("sdp_customers")
    return (
        orders.join(customers, "customer_id")
        .groupBy("tier", "country")
        .agg(
            F.countDistinct("customer_id").alias("customers"),
            F.count("*").alias("orders"),
            F.sum(F.col("amount").cast("decimal(18,2)")).alias("revenue"),
        )
    )
