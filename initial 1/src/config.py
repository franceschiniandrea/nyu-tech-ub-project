# config.py

# Supported exchanges available on Tardis Machine
EXCHANGES = ["hyperliquid",  "poloniex", "coinbase"]  # Add more if needed

# Base tickers we want to track (Tardis Machine format)
BASE_TICKERS = [
    "OP_USDT", "SC_USDT", "LDO_USDT", "SUI_USDT", "NEAR_USDT",
    "DASH_USDT", "ATOM_USDT", "STEEM_USDT", "UNI_USDT", "PEPE_USDT",
    "BNB_USDT", "LINK_USDT", "BTC_USDT", "ETH_USDT"
]

# Data types to stream from Tardis Machine
DATA_TYPES = ["trade", "book_snapshot_10_100ms"]

DB_CONFIG = {
    "database": "tardis_db",
    "user": "tardis_user",
    "password": "unsecuredpassword",
    "host": "localhost",
    "port": 5432
}