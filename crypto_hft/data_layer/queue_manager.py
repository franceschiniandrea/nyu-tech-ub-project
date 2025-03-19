# crypto_hft/data_layer/queue_manager.py
import asyncio
from crypto_hft.utils.config import Config


config = Config()

order_book_queues: dict[str, asyncio.Queue] = {symbol: asyncio.Queue() for symbol in config.base_tickers}
trade_queues: dict[str, asyncio.Queue] = {symbol: asyncio.Queue() for symbol in config.base_tickers}