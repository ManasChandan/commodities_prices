import json
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, functions as F
from delta.tables import DeltaTable

# Singleton Spark Session
spark = SparkSession.builder.getOrCreate()


def run_sql_file(file_path: str) -> DataFrame:
    """
    Reads a SQL query from a file and executes it within the Spark session.

    Args:
        file_path (str): Absolute or relative path to the .sql file.

    Returns:
        DataFrame: The resulting PySpark DataFrame from the query execution.
    """
    with open(file_path, "r") as f:
        sql_query = f.read()

    return spark.sql(sql_query)


def read_csv_with_schema(
    file_path: str, schema_json_string: str, date_format: str
) -> DataFrame:
    """
    Parses a JSON schema string to read a CSV into a structured DataFrame.

    Args:
        file_path (str): Path to the source CSV file.
        schema_json_string (str): JSON representation of a StructType schema.
        date_format (str): The format used to parse date columns (e.g., 'yyyy-MM-dd').

    Returns:
        DataFrame: DataFrame enforced with the provided schema.
    """
    # Convert JSON schema string to PySpark StructType
    schema = T.StructType.fromJson(json.loads(schema_json_string))

    return spark.read.csv(
        file_path,
        header=True,
        inferSchema=False,
        mode="FAILFAST",
        schema=schema,
        dateFormat=date_format,
    )


def merge_with_commodity(commodity_table_path, source_df, pipeline_time):
    target_table = DeltaTable.forPath(spark, commodity_table_path)

    source_active_changes = (
        source_df.alias("s")
        .join(
            target_table.toDF().alias("t"),
            (F.col("s.id") == F.col("t.id")) & (F.col("t.is_current")),
            "left",
        )
        .where(
            (F.col("t.id").isNull())
            | (F.col("s.commodity_name") != F.col("t.commodity_name"))
        )
        .select(
            "s.*",
            F.col("t.id").alias("target_id"),
            F.col("t.effective_start_timestamp").alias("effective_start_timestamp"),
        )
    )

    expiration_columns = {
        "merge_key": F.col("id"),
        "effective_start_timestamp": F.col("effective_start_timestamp"),
    }

    # Only records that passed the check above will be processed.
    updates_to_expire = source_active_changes.filter(
        F.col("target_id").isNotNull()
    ).withColumns(expiration_columns)

    active_changes_columns = {
        "merge_key": F.lit(None),
        "effective_start_timestamp": F.when(
            F.col("target_id").isNull(), F.lit("0001-01-01 00:00:00")
        ).otherwise(F.col("effective_start_timestamp")),
    }

    source_active_changes = source_active_changes.withColumns(active_changes_columns)

    staged_df = updates_to_expire.unionByName(
        source_active_changes, allowMissingColumns=True
    )

    # Since we pre-filtered, every row in staged_df is a guaranteed action.
    target_table.alias("t").merge(
        source=staged_df.alias("s"),
        condition="t.id = s.merge_key AND t.is_current = true",
    ).whenMatchedUpdate(
        set={
            "is_current": "false",
            "effective_end_timestamp": F.lit(pipeline_time),
            "updated_timestamp": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values={
            "id": "s.id",
            "commodity_name": "s.commodity_name",
            "effective_start_timestamp": "s.effective_start_timestamp",
            "effective_end_timestamp": F.lit("9999-12-31 23:59:59"),
            "is_current": "true",
            "updated_timestamp": F.current_timestamp(),
        }
    ).execute()
