from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Preprocess the CDC feed before applying it (temporary view = not published)
@dp.temporary_view()
@dp.expect_or_drop("valid_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE')")
def customers_cdc_clean():
    return (
        spark.readStream.table("sdp_bronze_customers_cdc")
        .select("customer_id", "name", "email", "country", "tier", "operation", "updated_at")
        .filter(F.col("customer_id").isNotNull())
    )


# SCD Type 1: current state of each customer (upserts + deletes, out-of-order safe)
dp.create_streaming_table(name="sdp_customers", comment="Current customer state (SCD Type 1) via AUTO CDC")
dp.create_auto_cdc_flow(
    target="sdp_customers",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by="updated_at",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation"],
    stored_as_scd_type=1,
)


# SCD Type 2: full history with __START_AT / __END_AT, tracking tier & country changes
dp.create_streaming_table(name="sdp_customers_history", comment="Customer history (SCD Type 2) via AUTO CDC")
dp.create_auto_cdc_flow(
    target="sdp_customers_history",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by="updated_at",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation"],
    stored_as_scd_type=2,
    track_history_column_list=["tier", "country"],
)
