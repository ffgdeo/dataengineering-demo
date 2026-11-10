# Data Engineering on Databricks: Demo

An end-to-end, self-contained demo of the main ways to ingest and transform data on Databricks:
**PySpark & SQL**, **Structured Streaming + Auto Loader**, **Lakeflow Spark Declarative Pipelines (SDP)** and **Lakeflow Connect**.

The storyline is *the same problem, solved with less and less code*: first everything by hand (batch, MERGE, streaming checkpoints, quarantine logic), then declaratively with SDP (expectations, AUTO CDC, materialized views), and finally managed ingestion with Lakeflow Connect.

All data is generated mock data (orders, customer CDC feed, clickstream events, products), no external systems needed.

## Repository layout

```
.
├── databricks.yml                     # Declarative Automation Bundle: deploys the SDP pipeline
├── notebooks/
│   ├── 00_Setup_Mock_Data             # creates schema, volumes and mock data (run first)
│   ├── 01_Data_Transformation_Overview  # use cases & limitations + PySpark vs SQL + manual MERGE
│   ├── 02_Structured_Streaming_Auto_Loader  # Auto Loader, watermarks, foreachBatch sink, quarantine
│   └── 03_Lakeflow_Connect            # managed connectors: concepts, limits, pipeline specs as code
└── pipeline/transformations/          # SDP pipeline source (Python)
    ├── bronze/   sdp_bronze_orders, sdp_bronze_customers_cdc, sdp_bronze_events, sdp_products
    ├── silver/   sdp_silver_orders (expectations), sdp_customers (AUTO CDC SCD1 + SCD2)
    └── gold/     sdp_gold_daily_sales, sdp_gold_revenue_by_tier, sdp_gold_events_hourly
```

## Prerequisites

* A Databricks workspace with **Unity Catalog** and **serverless compute** (notebooks and pipelines)
* A catalog where you can `CREATE SCHEMA` (or an existing schema where you can create tables and volumes)
* For the bundle deployment: a recent [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) authenticated to your workspace (or deploy from the workspace UI, see below)

## Quick start

1. **Get the code into your workspace**: *Workspace > Create > Git folder* and paste this repository URL.
2. **Generate the data**: open `notebooks/00_Setup_Mock_Data`, set the `catalog` and `schema` widgets (defaults: `main` / `de_demo`) and *Run all* on serverless.
3. **Batch transformations**: run `notebooks/01_Data_Transformation_Overview` with the same widget values.
4. **Structured Streaming**: run `notebooks/02_Structured_Streaming_Auto_Loader`. Use the *Land a new events file* cell and re-run the Auto Loader cell to show incremental processing.
5. **Deploy and run the SDP pipeline** (pick one):
   * **CLI**:
     ```bash
     databricks bundle deploy --var="catalog=<your_catalog>" --var="schema=de_demo"
     databricks bundle run de_demo_sdp_pipeline --var="catalog=<your_catalog>" --var="schema=de_demo"
     ```
   * **Workspace UI**: open the Git folder, click the *Deployments* (bundle) icon, set the `catalog` variable and click *Deploy*, then run the pipeline.
6. **Managed ingestion**: walk through `notebooks/03_Lakeflow_Connect`. Pipeline specs are only printed by default (`CREATE_PIPELINES = False`); plug in a real connection to create them.

## What to expect

| Dataset | Rows | Notes |
| --- | --- | --- |
| Raw orders | 5,000 | 97 invalid rows (null customer or negative amount) |
| Clean orders (PySpark, SQL, streaming, SDP silver) | 4,903 | All approaches return identical results |
| Customers (current state, SCD1) | 190 | 200 inserts, 40 updates, 10 deletes |
| Customer history (SCD2) | 240 rows / 190 current | `__START_AT` / `__END_AT` validity columns |
| Clickstream events | 10,000 | ~3% late events to illustrate watermarks |

## Choosing an approach (summary)

| Approach | Use it for | Watch out for |
| --- | --- | --- |
| PySpark / SQL batch | Ad-hoc, complex logic, ML prep, analytics engineering | You own incremental logic, idempotency and orchestration |
| Structured Streaming | Custom sinks, multi-sink writes, low latency, custom stateful logic | You manage checkpoints, schema evolution, DQ and restarts |
| Lakeflow SDP | Medallion ETL, incremental ingestion, CDC / SCD, data quality | Declarative model (no side effects), pipeline-owned tables |
| Lakeflow Connect | SaaS and database ingestion with managed incremental sync / CDC | Connector coverage, source API quotas, DB CDC prerequisites |

## Notes

* On serverless, `trigger(availableNow=True)` is used for all streams (process what is available, then stop).
* Windowed aggregations use `outputMode("append")`: a window appears only once the watermark has passed it, so the newest windows show up after more data arrives.
* While building this demo, a MERGE inside `foreachBatch` made the serverless session unusable, so notebook 02 avoids it. Upserts are shown with MERGE in notebook 01 and AUTO CDC in the pipeline.
* Reset: the last cell of notebook 02 drops the streaming demo tables and checkpoints; drop the schema to remove everything.

## Disclaimer

Sample code for demonstration purposes only, provided as-is without warranty. Review and adapt before using in production.
