-- Databricks notebook source

CREATE TABLE IF NOT EXISTS commodity_prices.commod_silver_layer.commodity_prices_event (
  date_of_observation DATE,
  commodity_name STRING,
  price_in_usd DECIMAL(18, 6),
  source_file_path STRING,
  file_ingestion_time TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (commodity_name, date_of_observation)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7'
);

-- Add comments for data governance
COMMENT ON TABLE commodity_prices.commod_silver_layer.commodity_prices_event 
IS 'Raw commodity price observations with ingestion metadata.';