# Individual Schema Definitions
COMMODITY_PRICE = {
    "type": "struct",
    "fields": [
        {"name": "Date", "type": "date", "nullable": False, "metadata": {}},
        {"name": "Value", "type": "double", "nullable": False, "metadata": {}},
    ],
}

ECONOMY_INDICATOR = {
    "type": "struct",
    "fields": [
        {"name": "Date", "type": "date", "nullable": False, "metadata": {}},
        {"name": "Value", "type": "double", "nullable": False, "metadata": {}},
    ],
}

DATA_GPR_EXPORT = {
    "type": "struct",
    "fields": [
        {"name": "Date", "type": "date", "nullable": False, "metadata": {}},
        {"name": "GPRC_USA", "type": "double", "nullable": True, "metadata": {}},
        {"name": "GPRHC_USA", "type": "double", "nullable": True, "metadata": {}},
    ],
}


# LOCATIONS

FULL_LOAD_PATH = "/opt/landing/full_load"

COMMODITY_TABLE_PATH = "/opt/warehouse/commodities"
COMMODITY_PRICE_TABLE_PATH = "/opt/warehouse/commodity_prices"
ECONOMY_INDICATOR_TABLE_PATH = "/opt/warehouse/economy_indicators"
ECONOMY_INDICATOR_NAME_TABLE_PATH = "/opt/warehouse/economy_indicator_names"


# FILE LANDING MAPPING

COMMODITIES_LANDING_FILENAMES = {
    "gold" : "gold_daily_price.csv", 
    "silver" : "silver_daily_price.csv", 
    "copper" : "copper_daily_price.csv",
    "platinum" : "platinum_daily_price.csv"
}

ECONOMY_LANDING_FILENAMES = {
    "geopolitical_risk" : "geopolitical_risk.csv",
    "debt_by_gdp" : "debt_by_gdp.csv",
    "national_dept" : "national_dept.csv",
    "inflation_rates" : "inflation_rates.csv"
}