from pyspark import pipelines as dp

LANDING = spark.conf.get("landing_path")


# Small static reference data -> materialized view (batch read, no streaming)
@dp.materialized_view(comment="Product dimension loaded from CSV")
@dp.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
def sdp_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{LANDING}/products")
    )
