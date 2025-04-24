import asyncio
import logging
import time
import asyncpg  
from crypto_hft.utils.config import Config
from crypto_hft.spot.queue_manager import order_book_queues, trade_queues
from crypto_hft.utils.time_utils import iso8601_to_datetime
from datetime import datetime
from ciso8601 import parse_datetime


class PostgreSQLDatabase:
    """Handles async PostgreSQL connections and batch inserts."""
    
    def __init__(self, config: Config):
        self.pool = None
        self.config = config

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.config.postgres_host,
            user=self.config.postgres_user,
            password=self.config.postgres_password,
            database=self.config.postgres_database,
            port=self.config.postgres_port,
            min_size=1,
            max_size=10
        )
        logging.info("✅ Async PostgreSQL Connection Established")

    async def close(self):
        await self.pool.close()
        logging.info("[!] PostgreSQL connection closed.")

    async def insert_batch(self, table_name: str, batch_data: list, columns: list):
        """Insert data asynchronously using asyncpg."""
        if not batch_data:
            return

        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        col_names = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, batch_data)

        logging.info(f"[✅] Inserted {len(batch_data)} rows into {table_name}")

# --------------------------------------------

class QueueProcessor:
    """Handles queue processing and batch insertion into PostgreSQL."""

    def __init__(self, db: PostgreSQLDatabase, config: Config):
        self.db = db
        self.config = config
        self.shutdown_event = asyncio.Event()

    async def process_queue(self, symbol: str, queue: asyncio.Queue, table_prefix: str, columns: list):
        last_flush_time = time.time()

        while not self.shutdown_event.is_set():
            try:
                queue_size = queue.qsize()
                threshold = (
                    self.config.orderbook_queue_threshold
                    if table_prefix == "orderbook"
                    else self.config.trade_queue_threshold
                )

                if queue_size >= threshold:
                    batch_size = min(threshold, queue_size)
                    table_name = f"{table_prefix}_{symbol.lower()}"
                    batch_data = []

                    for _ in range(batch_size):
                        item = await queue.get()
                        item["timestamp"] = iso8601_to_datetime(item["timestamp"])
                        item["local_timestamp"] = iso8601_to_datetime(item["local_timestamp"])
                        batch_data.append(tuple(item[col] for col in columns))

                    await self.db.insert_batch(table_name, batch_data, columns)
                    last_flush_time = time.time()

                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"[❌] Insert Error for {symbol}: {e}")

    async def process_order_book_queue(self, symbol: str, queue: asyncio.Queue):
        columns = ["exchange", "timestamp", "local_timestamp"] + [
            f"bid_{i}_sz" for i in range(self.config.orderbook_levels)
        ] + [
            f"bid_{i}_px" for i in range(self.config.orderbook_levels)
        ] + [
            f"ask_{i}_sz" for i in range(self.config.orderbook_levels)
        ] + [
            f"ask_{i}_px" for i in range(self.config.orderbook_levels)
        ]
        await self.process_queue(symbol, queue, "orderbook", columns)

    async def process_trade_queue(self, symbol: str, queue: asyncio.Queue):
        columns = ["exchange", "trade_id", "price", "amount", "side", "timestamp", "local_timestamp"]
        await self.process_queue(symbol, queue, "trade", columns)

    async def batch_insert_order_books(self):
        tasks = [asyncio.create_task(self.process_order_book_queue(symbol, queue)) for symbol, queue in order_book_queues.items()]
        await asyncio.gather(*tasks)

    async def batch_insert_trades(self):
        tasks = [asyncio.create_task(self.process_trade_queue(symbol, queue)) for symbol, queue in trade_queues.items()]
        await asyncio.gather(*tasks)

    async def shutdown(self):
        logging.info("[!] Stopping queue processor...")
        self.shutdown_event.set()
