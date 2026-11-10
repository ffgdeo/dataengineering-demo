from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Silver: cleaned orders. Expectations drop invalid rows and record DQ metrics
# in the pipeline event log (visible in the pipeline UI).
@dp.table(comment="Validated orders; invalid customer_id / negative amounts are dropped")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect_or_drop("non_negative_amount", "amount >= 0")
@dp.expect("known_status", "status IN ('placed', 'shipped', 'delivered', 'returned')")
def sdp_silver_orders():
    return (
        spark.readStream.table("sdp_bronze_orders")
        .select(
            "order_id", "customer_id", "product_id", "quantity", "amount", "status", "order_ts",
            F.to_date("order_ts").alias("order_date"),
        )
    )
