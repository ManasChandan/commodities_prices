# Databricks notebook source
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    spark: Any = None
    dbutils: Any = None

from datetime import datetime
import metadata_operations as mo
import raw_file_utils as rfu

# COMMAND ----------

pipeline_run_satrt_time = datetime.now()

pipline_run_context = mo.get_pipeline_context(dbutils)

volume_path = "/Volumes/commodity_prices/commod_bronze_layer/price_landing_zone"

# COMMAND ----------

last_run_time = mo.get_latest_passed_run_timestamp(spark, pipline_run_context["pipeline_name"])

input_df = rfu.get_optimized_commodity_data(spark, volume_path, last_run_time)
