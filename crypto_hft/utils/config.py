from dotenv import load_dotenv
import os

SECRETS = [
    'db_host',
    'db_user', 
    'db_password', 
    'db_database'
]


class Config():
    """Define config for the whole project, and pull secrets when initializing the class"""
    # tickers to stream
    exchanges = ['coinbase','hyperliquid','poloniex']
    base_tickers = [
    "BTC_USDT",  # Bitcoin
    "ETH_USDT",  # Ethereum
    "XRP_USDT",  # XRP
    "SOL_USDT",  # Solana
    "DOGE_USDT", # Dogecoin
    "ADA_USDT",  # Cardano
    "TRX_USDT",  # TRON
    "LTC_USDT",  # Litecoin
    "LINK_USDT"  # Chainlink
    ]
    data_types = [
     'book_snapshot_10_0s', 'trade'
    ]
    
    
    db_host = None
    db_user = None
    db_password = None
    db_database = None
    db_port = 3306
    
    #orderbook configuration
    orderbook_levels = 15

    # Retry configuration for WebSocket connection
    max_retries = 5
    retry_wait_time = 10  # seconds


    # Batch insert configuration
    orderbook_queue_threshold = 10000 
    trade_queue_threshold = 5000 

    def __init__(self): 
        load_dotenv()

        for secret in SECRETS:
            secret_name = secret.upper()
            if os.getenv(secret_name) is None:
                raise ValueError(
                    f"Missing secret: {secret_name}."
                    "Please create a .env file in the root directory of the project"
                    f" and add the following env variables to it: {SECRETS}"
                )
            setattr(self, secret, os.getenv(secret_name))

config = Config()



