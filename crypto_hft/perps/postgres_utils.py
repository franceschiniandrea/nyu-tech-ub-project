import asyncio
import logging
from datetime import datetime
from timeutils import iso8601_to_unix
import asyncpg

from gcs_fallback_writer import GCSFallbackWriter  # ✅ NEW

class PostgreSQLDatabase:
    def __init__(self, config):
        self.config = config
        self.pool = None

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

    async def insert_batch(self, table_name, batch_data, columns):
        if not batch_data:
            return
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, batch_data)
        logging.info(f"[✅] Inserted {len(batch_data)} rows into {table_name}")


class QueueProcessor:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.shutdown_event = asyncio.Event()
        self.gcs_writer = GCSFallbackWriter(config.gcs_bucket)  # ✅ NEW

    async def process_queue(self, symbol, queue, table_prefix, columns):
        while not self.shutdown_event.is_set():
            try:
                threshold = (
                    self.config.orderbook_queue_threshold
                    if table_prefix == "orderbook" else
                    self.config.trade_queue_threshold
                )
                if queue.qsize() >= threshold:
                    table_name = f"{table_prefix}_perps_{symbol.lower()}"
                    batch_data = []
                    for _ in range(min(threshold, queue.qsize())):
                        item = await queue.get()
                        for ts_field in ["timestamp", "local_timestamp"]:
                            if isinstance(item[ts_field], str):
                                item[ts_field] = datetime.utcfromtimestamp(iso8601_to_unix(item[ts_field]))
                            elif isinstance(item[ts_field], (int, float)):
                                item[ts_field] = datetime.utcfromtimestamp(item[ts_field] / 1000)
                        batch_data.append(tuple(item[col] for col in columns))

                    try:
                        await self.db.insert_batch(table_name, batch_data, columns)
                    except Exception as e:
                        logging.error(f"[❌] Insert Error for {symbol}: {e}")
                        logging.warning(f"[⏳] Fallback to GCS for {symbol}...")
                        self.gcs_writer.save_and_upload(symbol, table_prefix, columns, batch_data)

                await asyncio.sleep(0.1)

            except Exception as e:
                logging.error(f"[❌] Queue Processing Error for {symbol}: {e}")

    async def process_order_book_queue(self, symbol, queue):
        levels = self.config.orderbook_levels
        columns = ["exchange", "timestamp", "local_timestamp"] + \
                  [f"bid_{i}_sz" for i in range(levels)] + \
                  [f"bid_{i}_px" for i in range(levels)] + \
                  [f"ask_{i}_sz" for i in range(levels)] + \
                  [f"ask_{i}_px" for i in range(levels)]
        await self.process_queue(symbol, queue, "orderbook", columns)

    async def process_trade_queue(self, symbol, queue):
        columns = ["exchange", "trade_id", "price", "amount", "side", "timestamp", "local_timestamp"]
        await self.process_queue(symbol, queue, "trade", columns)

    async def batch_insert_order_books(self, queues):
        await asyncio.gather(*[
            asyncio.create_task(self.process_order_book_queue(symbol, queue))
            for symbol, queue in queues.items()
        ])

    async def batch_insert_trades(self, queues):
        await asyncio.gather(*[
            asyncio.create_task(self.process_trade_queue(symbol, queue))
            for symbol, queue in queues.items()
        ])

    async def shutdown(self):
        logging.info("[!] Stopping queue processor...")
        self.shutdown_event.set()
