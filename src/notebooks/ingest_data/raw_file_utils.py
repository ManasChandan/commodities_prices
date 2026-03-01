from typing import TYPE_CHECKING, Any
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DateType, DecimalType

if TYPE_CHECKING:
    spark: Any = None

# --- Configuration & Schema ---
CSV_FILE_SCHEMA = StructType([
    StructField("Date", DateType(), True),
    StructField("Value", DecimalType(18, 6), True),
])

def get_optimized_commodity_data(spark, bronze_volume_path: str, last_run_time: datetime):
    """
    Optimized to read all commodity folders in parallel.
    Captures the actual file arrival time using Spark's hidden metadata.
    """
    
    # Convert comparison date to ISO 8601 string for the Spark optimizer
    spark_time_str = last_run_time.isoformat()

    # Read using Spark's cloud-native file listing
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("modifiedAfter", spark_time_str)
        .option("recursiveFileLookup", "true")
        .schema(CSV_FILE_SCHEMA)
        # We select the hidden _metadata column to get the actual file timestamp
        .load(f"{bronze_volume_path}/*")
        .select("*", "_metadata.*") 
    )

    # Transform and clean
    final_df = (
        raw_df.select(
            # Business Columns
            F.col("Date").alias("date_of_observation"),
            F.col("Value").alias("price_in_usd"),
            
            # Metadata Columns
            F.col("file_path").alias("source_file_path"),
            # 'file_modification_time' is the actual arrival/last-edit time in the Volume
            F.col("file_modification_time").alias("file_ingestion_time"),
            
            # Extract commodity folder name from path
            F.element_at(F.split(F.col("file_path"), "/"), -2).alias("commodity_name")
        )
        # Final cleanup logic
        .dropDuplicates()
        .filter(F.col("price_in_usd") > 0) # Usually prices should be positive
    )

    return final_df