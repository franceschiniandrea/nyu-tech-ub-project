from dotenv import load_dotenv
import os

SECRETS = [
    'mysql_host',
    'mysql_user', 
    'mysql_password', 
    'mysql_database',
    'telegram_api_key',
    'telegram_chat_id'
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
     'book_snapshot_15_0s', 'trade'
    ]
    
    mysql_host = None
    mysql_user = None
    mysql_password = None
    mysql_database = None
    mysql_port = 3306

    # telegram logger configuration 
    telegram_api_key: str | None = None
    telegram_chat_id: str | None = None
    
    #orderbook configuration
    orderbook_levels = 15

    # Retry configuration for WebSocket connection
    max_retries = 5
    retry_wait_time = 10  # seconds

    # Batch insert configuration
    orderbook_queue_threshold = 20000 
    trade_queue_threshold = 5000 

    # logging configuration     
    logger_telegram_min_level = 'WARNING'
    logger_file_min_level = 'TRACE'
    logger_console_min_level = 'DEBUG'
    logger_telegram_max_buffer = 10
    logger_file_filepath = 'logs.log'

    def __init__(self): 
        load_dotenv()

        # get secrets from environment variables
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



