import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession

sys.path.append(os.getcwd())

from utils import utils as u
from config import config as c

spark = SparkSession.builder.getOrCreate()



def create_raw_tables():

    current_file_direcotry = os.path.dirname(os.path.abspath(__file__))

    u.run_sql_file(
        os.path.join(current_file_direcotry, "sql", "create_commodities.sql")
    )

    u.run_sql_file(
        os.path.join(current_file_direcotry, "sql", "create_commodity_prices.sql")
    )

    return None

def create_commodity_dimensions_data():

    all_commodities = list(c.COMMODITIES_LANDING_FILENAMES.keys())

    all_commodities_id = [i + 1 for i in range(len(all_commodities))]

    commodities_df = spark.createDataFrame(
        zip(all_commodities_id, all_commodities),
        ["id", "commodity_name"],
    )

    return commodities_df

def full_load():

    pipeline_run_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    create_raw_tables()

    commodities_df = create_commodity_dimensions_data()

    u.merge_with_commodity(
        commodity_table_path=c.COMMODITY_TABLE_PATH,
        source_df=commodities_df,
        pipeline_time=pipeline_run_datetime
    )

full_load()