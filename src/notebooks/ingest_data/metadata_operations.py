from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType


def get_latest_passed_run_timestamp(spark, pipeline_name: str | None) -> datetime:
    """
    Returns the latest run_timestamp for a given pipeline where is_passed is True.
    If no records are found, returns 0001-01-01 00:00:00.
    """

    if pipeline_name is None:
        return datetime.min

    table_name = "commodity_prices.commod_silver_layer.pipeline_run_info"

    # Filter for the specific pipeline and successful runs
    df = (
        spark.table(table_name)
        .filter((F.col("pipeline_name") == pipeline_name) & F.col("is_passed"))
        .select(F.max("run_timestamp").alias("latest_run"))
    )

    # Collect the result
    row = df.collect()[0]
    result = row["latest_run"]

    # Return the result if it exists, otherwise return the minimum possible datetime
    return result if result is not None else datetime.min


def log_metadata_to_delta(spark, metadata_dict):
    """
    Converts a dictionary to a row and appends it to a Delta Table.

    :param table_name: Full three-level name (catalog.schema.table)
    :param metadata_dict: Dictionary containing column names as keys
    """
    schema = StructType([
        StructField("pipeline_name", StringType(), True),
        StructField("run_id", IntegerType(), True),
        StructField("run_timestamp", TimestampType(), True),
        StructField("is_passed", BooleanType(), True),
        StructField("rows_processed", IntegerType(), True)
    ])

    # 1. Create a single-row DataFrame from the dict
    # Using [metadata_dict] inside spark.createDataFrame is the cleanest way
    new_row_df = spark.createDataFrame([metadata_dict], schema=schema)

    # 2. Append to the Delta Table
    (
        new_row_df.write.format("delta")
        .mode("append")
        .saveAsTable("commodity_prices.commod_silver_layer.pipeline_run_info")
    )


def get_pipeline_context(dbutils) -> dict:
    """
    Retrieves the current pipeline name and run ID from the Databricks context.
    Works for both Jobs/Workflows and interactive notebooks.
    """
    # Access the internal notebook context
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    # Extract Pipeline/Job Name
    # 'notebook_path' is often used as the name in interactive sessions
    # 'jobName' is populated when running via a Workflow
    pipeline_name = context.jobName().toString()

    # Extract Run ID
    # 'runId' is populated during Job runs; otherwise, we use the tag 'multitaskParentRunId'
    run_id = context.runId().toString()

    return {
        "pipeline_name": pipeline_name if pipeline_name != "None" else None,
        "run_id": run_id if run_id != "None" else None,
    }
