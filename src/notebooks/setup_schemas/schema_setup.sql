-- Databricks notebook source

-- COMMAND ----------

-- Create the main catalog
CREATE CATALOG IF NOT EXISTS commodity_prices;

-- Create the Bronze layer for raw ingestion
CREATE SCHEMA IF NOT EXISTS commodity_prices.commod_bronze_layer
COMMENT 'Raw landing zone for ingested commodity data';

-- Create the Silver layer for cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS commodity_prices.commod_silver_layer
COMMENT 'Refined and filtered commodity price data';

-- Create the Gold layer for final business reporting
CREATE SCHEMA IF NOT EXISTS commodity_prices.commod_gold_layer
COMMENT 'Fact, Dimensions and the analytical tables';

-- Create a managed volume for raw file storage
CREATE VOLUME IF NOT EXISTS commodity_prices.commod_bronze_layer.price_landing_zone
COMMENT 'External file storage for incoming commodity price files (CSV)';