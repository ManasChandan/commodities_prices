# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Ingestion: Commodity Prices
# MAGIC Refines raw data from Bronze/Landing and performs an Upsert (Merge) into the Silver event table.

# COMMAND ----------
from typing import TYPE_CHECKING, Any
from datetime import datetime

if TYPE_CHECKING:
    spark: Any = None
    dbutils: Any = None

# COMMAND ----------
# 1. IMPORTS
from delta.tables import DeltaTable
import pyspark.sql.functions as F

import metadata_operations as mo
import raw_file_utils as rfu

# COMMAND ----------
# 2. CONFIGURATION & CONTEXT
# Centralizing paths makes the script easier to maintain
TARGET_TABLE_NAME = "commodity_prices.commod_silver_layer.commodity_prices_event"
VOLUME_PATH = "/Volumes/commodity_prices/commod_bronze_layer/price_landing_zone"

run_start_time = datetime.now()
pipeline_context = mo.get_pipeline_context(dbutils)
pipeline_name = pipeline_context["pipeline_name"]

# COMMAND ----------
# 3. DATA PREPARATION
# Fetch the last successful sync point
last_run_time = mo.get_latest_passed_run_timestamp(spark, pipeline_name)

# Read incremental data
input_df = rfu.get_optimized_commodity_data(spark, VOLUME_PATH, last_run_time)

# Add audit metadata
input_df = input_df.withColumn("created_at", F.current_timestamp()).withColumn(
    "updated_at", F.current_timestamp()
)

# COMMAND ----------
# 4. UPSERT (MERGE) LOGIC
# TODO: Run ID Handling and datatype change
target_table = DeltaTable.forName(spark, TARGET_TABLE_NAME)

status = "FAILED"
rows_affected = 0

try:
    # Perform Merge
    (
        target_table.alias("target")
        .merge(
            input_df.alias("source"),
            "target.commodity_name = source.commodity_name AND target.date_of_observation = source.date_of_observation",
        )
        .whenMatchedUpdate(
            set={
                "price_in_usd": "source.price_in_usd",
                "source_file_path": "source.source_file_path",
                "file_ingestion_time": "source.file_ingestion_time",
                "updated_at": F.current_timestamp(),
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    status = "PASSED"
    rows_affected = input_df.count()

except Exception as e:
    print(f"Error during Merge operation for {pipeline_name}: {e}")
    status = "FAILED"

finally:
    # 5. METADATA LOGGING
    metadata_log = {
        "pipeline_name": pipeline_name,
        "run_id": 23,
        "run_timestamp": run_start_time,
        "is_passed": (status == "PASSED"),
        "rows_processed": rows_affected,
    }

    mo.log_metadata_to_delta(spark, metadata_log)

    if status != "PASSED":
        raise Exception(f"Pipeline {pipeline_name} failed. Check logs for details.")
