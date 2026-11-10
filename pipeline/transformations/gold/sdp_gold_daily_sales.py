from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Gold: daily revenue by category. Materialized view over streaming tables;
# on serverless it refreshes incrementally when possible.
@dp.materialized_view(comment="Daily orders and revenue by product category")
def sdp_gold_daily_sales():
    orders = spark.read.table("sdp_silver_orders")
    products = spark.read.table("sdp_products")
    return (
        orders.join(products, "product_id")
        .groupBy("order_date", "category")
        .agg(
            F.count("*").alias("orders"),
            F.sum(F.col("amount").cast("decimal(18,2)")).alias("revenue"),
        )
    )
