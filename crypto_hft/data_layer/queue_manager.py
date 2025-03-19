# crypto_hft/data_layer/queue_manager.py
import asyncio
from crypto_hft.utils.config import Config
import logging 

config = Config()

order_book_queues: dict[str, asyncio.Queue] = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in config.base_tickers}
trade_queues: dict[str, asyncio.Queue] = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in config.base_tickers}

# print(f"[DEBUG] Available Order Book Queues: {list(order_book_queues.keys())}")
# print(f"[DEBUG] Available Trade Queues: {list(trade_queues.keys())}")
