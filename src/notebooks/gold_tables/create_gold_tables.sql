-- Databricks notebook source

-- Set the context to your gold layer
USE CATALOG commodity_prices;
USE SCHEMA commod_gold_layer;

-- COMMAND ----------
-- 1. Date Dimension
-- Essential for time-series analysis and joining with fact tables
CREATE TABLE IF NOT EXISTS date_dimension (
  dateid INT NOT NULL,
  date DATE NOT NULL,
  year INT,
  month INT,
  day_of_month INT,
  week_of_year INT,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (dateid)
COMMENT 'Time dimension for grain-level date reporting';

-- COMMAND ----------
-- 2. Commodity Dimension
-- SCD Type 2 (Slowly Changing Dimension) structure to track history
CREATE TABLE IF NOT EXISTS commodity_dimension (
  commodity_id INT NOT NULL,
  commodity_name STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (commodity_id)
COMMENT 'Master list of commodities with historical versioning';

-- COMMAND ----------
-- 3. Price Fact Table
-- The central table for daily price observations
CREATE TABLE IF NOT EXISTS price_fact (
  dateid INT NOT NULL,
  commodity_id INT NOT NULL,
  price DECIMAL(18, 6),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (commodity_id, dateid)
COMMENT 'Fact table containing daily commodity price observations';

-- COMMAND ----------
-- 4. Weekly YoY Analytics Table
-- Pre-calculated year-over-week percentage changes
CREATE TABLE IF NOT EXISTS yoy_weekly_change_ana (
  commodity_id INT NOT NULL,
  week_of_year INT NOT NULL,
  year INT NOT NULL,
  pct_change DECIMAL(5, 3)
)
USING DELTA
CLUSTER BY (commodity_id, year)
COMMENT 'Analytics table for Year-over-Year weekly price fluctuations';

-- COMMAND ----------
-- 5. Monthly YoY Analytics Table
-- Pre-calculated year-over-month percentage changes
CREATE TABLE IF NOT EXISTS yoy_monthly_change_ana (
  commodity_id INT NOT NULL,
  month INT NOT NULL,
  year INT NOT NULL,
  pct_change DECIMAL(5, 3)
)
USING DELTA
CLUSTER BY (commodity_id, year)
COMMENT 'Analytics table for Year-over-Year monthly price fluctuations';