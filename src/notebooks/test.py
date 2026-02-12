# Databricks notebook source
# MAGIC %md # My Spark Analysis

# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    spark = None
    display = None

df = spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------
display(df.limit(10))
