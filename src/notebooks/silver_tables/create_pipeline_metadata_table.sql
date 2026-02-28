-- Databricks notebook source

CREATE TABLE IF NOT EXISTS commodity_prices.commod_silver_layer.pipeline_run_info (
  pipeline_name STRING,
  run_id INT,
  run_timestamp TIMESTAMP,
  is_passed BOOLEAN,
  rows_processed INT
)
USING DELTA
CLUSTER BY (pipeline_name, run_timestamp)
TBLPROPERTIES (
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7'
);

-- Add comments for data governance
COMMENT ON TABLE commodity_prices.commod_silver_layer.pipeline_run_info 
IS 'Audit logs for pipeline execution tracking and row counts.';