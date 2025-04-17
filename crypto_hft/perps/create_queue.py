from crypto_hft.utils.config import Config
from normalizers import normalize_symbol
import asyncio

config = Config()
order_book_queues_perps: dict[str, asyncio.Queue] = {}
trade_queues_perps: dict[str, asyncio.Queue] = {}

for token in config.TARGET_TOKENS:
    # Use the actual market format you're streaming (e.g., "BTC/USDT:USDT" for perps)
    market = f"{token}/USDT:USDT"
    normalized = normalize_symbol(market)
    order_book_queues_perps[normalized] = asyncio.Queue()
    trade_queues_perps[normalized] = asyncio.Queue()
