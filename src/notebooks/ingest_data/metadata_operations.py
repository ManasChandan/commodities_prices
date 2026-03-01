from datetime import datetime
import pyspark.sql.functions as F


def get_latest_passed_run_timestamp(spark, pipeline_name: str|None) -> datetime:
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

    return {"pipeline_name": pipeline_name if pipeline_name != 'None' else None, 
            "run_id": run_id if run_id != 'None' else None}
