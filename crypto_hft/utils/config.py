from dotenv import load_dotenv
from pathlib import Path
import os

TARGET_TOKENS = [
    "BTC", "ETH", "XRP", "SOL", "DOGE",
    "ADA", "TRX", "LTC", "MATIC", "LINK",
    "OP", "SC", "LDO", "SUI", "NEAR"
]

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# Secrets that must exist in the .env file
SECRETS = [
    # Database
    'postgres_host',
    'postgres_user',
    'postgres_password',
    'postgres_database',

    # Telegram logging
    'telegram_api_key',
    'telegram_chat_id',

    # Exchange credentials
    'coinbase_api_key',
    'coinbase_api_secret',
    'poloniex_api_key',
    'poloniex_api_secret',
    'binance_api_key',
    'binance_api_secret',
]

class Config:
    """Global project config. Loads secrets and constants from .env and environment."""

    # Exchange support
    exchanges = ['coinbase', 'hyperliquid', 'poloniex', 'binance']
    data_types = [
        "trade", # all trades
        "book_change", # diffs
        "book_snapshot_20_30s" # OB snapshots, 20 levels every minute
    ]

    target_tokens = [
    "OP", 
    "SC", 
    "LDO", 
    "SUI", 
    "NEAR",
    "DASH",
    "ATOM", 
    "STEEM",
    "UNI",
    "PEPE",
    # "ZEC",  
    "BNB",
    "LINK", 
    "DOT",
    "BTC", 
    "ETH",
    "XRP", 
    "SOL", 
    "DOGE", 
    "ADA", 
    "TRX", 
    "LTC"
]
    # Tickers to track
    # TARGET_TOKENS = [
    # "BTC", "ETH", "XRP", "SOL", "DOGE",
    # "ADA", "TRX", "LTC", "MATIC", "LINK",
    # "OP", "SC", "LDO", "SUI", "NEAR"
    # ]

    base_tickers = [
    "BTC_USDT", 
    "ETH_USDT",  
    "XRP_USDT",  # XRP
    "SOL_USDT",  
    "DOGE_USDT", # Dogecoin
    "ADA_USDT",  # Cardano
    "TRX_USDT",  # TRON
    "LTC_USDT",  # Litecoin
    "LINK_USDT"  # Chainlink
    ]

    # Order book config
    orderbook_levels = 15

    # Retry logic
    max_retries = 5
    retry_wait_time = 10

    # Batching thresholds
    orderbook_queue_threshold = 20000
    trade_queue_threshold = 10000

    # Logging settings
    logger_telegram_min_level = 'WARNING'
    logger_file_min_level = 'TRACE'
    logger_console_min_level = 'DEBUG'
    logger_telegram_max_buffer = 10
    logger_file_filepath = 'logs.log'

    # Fallback storage
    # s3_bucket = os.getenv("S3_BUCKET_NAME")
    # s3_prefix = os.getenv("S3_FALLBACK_PREFIX")

    gcs_bucket = os.getenv("GCS_BUCKET")
    gcs_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # Postgres fields (populated in __init__)
    postgres_host = None
    postgres_user = None
    postgres_password = None
    postgres_database = None
    postgres_port = 5432

    # Telegram fields
    telegram_api_key: str | None = None
    telegram_chat_id: str | None = None

    def __init__(self): 
        load_dotenv()

        # Load required secrets from .env
        for secret in SECRETS:
            env_var = secret.upper()
            val = os.getenv(env_var)
            if val is None:
                raise ValueError(
                    f"❌ Missing secret: {env_var}. Please define it in your .env file."
                )
            setattr(self, secret, val)

        #Validate GCS key path if provided
        if self.gcs_key_path and not os.path.exists(self.gcs_key_path):
            raise ValueError(f"❌ GOOGLE_APPLICATION_CREDENTIALS path is invalid - {Path(self.gcs_key_path).resolve()}.")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.gcs_key_path

    @property
    def credentials(self) -> dict:
        """Returns a dictionary of all exchange credentials."""
        return {
            'coinbase_api_key': self.coinbase_api_key, # type: ignore
            'coinbase_api_secret': self.coinbase_api_secret, # type: ignore
            'poloniex_api_key': self.poloniex_api_key, # type: ignore
            'poloniex_api_secret': self.poloniex_api_secret, # type: ignore
            'binance_api_key': self.binance_api_key, # type: ignore
            'binance_api_secret': self.binance_api_secret, # type: ignore
        }