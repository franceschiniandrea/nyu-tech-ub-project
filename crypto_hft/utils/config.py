from dotenv import load_dotenv
import os

# define the secrets that need to be pulled from the .env file
SECRETS = [
    'db_name',
    'db_user', 
    'db_password', 
    'db_host',
    'db_port'
]

class Config():
    """Define config for the whole project, and pull secrets when initializing the class"""
    # tickers to stream
    exchanges = ['coinbase', 'hyperliquid', 'poloniex']
    base_tickers = [
        "BTC_USDT",
        "ETH_USDT"
    ]
    data_types = [
        'trade', 'book_snapshot_10_0ms'
    ]

    db_name = None
    db_user = None
    db_password = None
    db_host = None
    db_port = None

    def __init__(self): 
        load_dotenv()

        for secret in SECRETS:
            secret_name = secret.upper()
            if os.getenv(secret) is None:
                raise ValueError(
                    f"Missing secret: {secret_name}."
                    "Please create a .env file in the root directory of the project"
                    f" and add the following env variables to it: {SECRETS}"
                )
            setattr(self, secret, os.getenv(secret_name))