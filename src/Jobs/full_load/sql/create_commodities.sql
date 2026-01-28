CREATE TABLE IF NOT EXISTS commodities (
    id INT,
    commodity_name STRING,
    effective_start_timestamp TIMESTAMP,
    effective_end_timestamp TIMESTAMP,
    is_current BOOLEAN,
    updated_timestamp TIMESTAMP
)
USING DELTA
LOCATION '/opt/warehouse/commodities';