# Databricks notebook source
# MAGIC %md
# MAGIC # Main Notebook to create all the silver tables. 

# COMMAND ----------

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # These variables are provided globally by the Databricks runtime
    # Defining them as 'Any' here prevents linting errors in local IDEs
    spark: Any = None
    dbutils: Any = None

# COMMAND ----------

silver_table_create_notebooks = [
    "create_commodity_prices_events", 
    "create_pipleine_metadata_table"
]

for silver_table in silver_table_create_notebooks:
    dbutils.notebook.run(silver_table, 300)