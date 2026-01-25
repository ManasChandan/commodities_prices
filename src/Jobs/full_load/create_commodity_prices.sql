CREATE TABLE IF NOT EXISTS commodity_prices (
    Date DATE,
    Commodity STRING,
    Price DOUBLE,
    Currency STRING
)
USING DELTA
CLUSTER BY (Commodity, Date)
LOCATION '/opt/warehouse/commodity_prices';