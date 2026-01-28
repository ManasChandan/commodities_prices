import json

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, functions as F
from delta.tables import DeltaTable

 # Creating a spark session, singleton class
spark = SparkSession.builder.getOrCreate()


def run_sql_file(file_path: str):
    """
    Runs a sql query written within a .sql file.

    Args:
        file_path (str): The path to the .sql file.

    Returns:
        DataFrame: The PySpark DataFrame.
    """

    # Read the SQL file
    with open(file_path, "r") as f:
        sql_query = f.read()

    # Execute and store the result as a DataFrame
    df = spark.sql(sql_query)

    # Retuns the dataframe
    return df


def read_csv_with_schema(
    file_path: str, schema_json_string: str, date_format: str
) -> DataFrame:
    """
    Reads a CSV file into a PySpark DataFrame with a specified schema and date format.

    Args:
        spark (SparkSession): The active SparkSession.
        file_path (str): The path to the CSV file.
        schema_json_string (str): A JSON string representing the PySpark schema (StructType).
        date_format (str): The format string for parsing dates (e.g., 'yyyy-MM-dd').

    Returns:
        DataFrame: The PySpark DataFrame.
    """

    # Creating a spark session, singleton class
    spark = SparkSession.builder.getOrCreate()

    # Convert JSON schema string to PySpark StructType
    schema = T.StructType.fromJson(json.loads(schema_json_string))

    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=False,
        mode="FAILFAST",
        schema=schema,
        dateFormat=date_format,
    )

    return df

def merge_with_commodity(
    commodity_table_path: str,
    commodity_df: DataFrame,
    pipeline_run_datetime: str
):
    
    commodity_table = DeltaTable.forPath(spark, commodity_table_path)

    commodity_table.alias("target").merge(
        commodity_df.alias("source"),
        "target.id = source.id and target.is_current = true",
    ).whenMatchedUpdate(
        condition="target.commodity_name != source.commodity_name and target.is_current = true",
        set={
            "is_current": F.lit(False),
            "effective_end_timestamp": F.lit(pipeline_run_datetime),
            "updated_timestamp": F.lit(pipeline_run_datetime),
        }
    ).execute()

    commodity_table_path.alias("target").merge(
        commodity_df.alias("source"),
        "target.id = source.id and target.is_current = false",
    ).whenNotMatchedInsertAll().execute()
            
