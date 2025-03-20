import asyncio
import logging
import time
import aiomysql # type: ignore
from aiomysql import Pool
from crypto_hft.utils.config import Config
from crypto_hft.data_layer.queue_manager import order_book_queues, trade_queues
from crypto_hft.utils.time_utils import iso8601_to_unix, unix_to_mysql_datetime

class MySQLDatabase:
    """Handles async MySQL connections and batch inserts."""

    def __init__(self, config: Config):
        self.pool : Pool | None = None
        self.config = config

    async def connect(self):
        """Create a connection pool for MySQL."""
        self.pool = await aiomysql.create_pool(
            host=self.config.db_host,
            user=self.config.db_user,
            password=self.config.db_password,
            db=self.config.db_database,
            port=self.config.db_port,
            autocommit=True
        )
        logging.info("✅ Async MySQL Connection Established")

    async def insert_batch(self, table_name: str, batch_data: list, columns: list):
        """Insert data asynchronously using aiomysql."""
        if not batch_data:
            return
        if self.pool is None: 
            raise Exception('self.pool is None')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(columns))
                column_names = ", ".join(columns)
                query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

                try:
                    await cursor.executemany(query, batch_data)
                    logging.info(f"[✅] Inserted {len(batch_data)} rows into {table_name}")
                except aiomysql.Error as e:
                    logging.error(f"[❌] Failed to insert batch into {table_name}: {e}")

    async def close(self):
        """Close the MySQL connection pool."""
        self.pool.close()
        await self.pool.wait_closed()
        logging.info("[!] MySQL connection closed.")


class QueueProcessor:
    """Handles queue processing and batch insertion into MySQL."""

    def __init__(self, db: MySQLDatabase, config: Config):
        self.db = db
        self.config = config
        self.shutdown_event = asyncio.Event()  


    async def process_queue(self, symbol: str, queue: asyncio.Queue, table_prefix: str, columns: list):
        """General function to process a queue and insert data into MySQL."""
        last_flush_time = time.time()

        while not self.shutdown_event.is_set():
            try:
                queue_size = queue.qsize()
                elapsed_time = time.time() - last_flush_time

                # Use different thresholds for trade vs orderbook
                threshold = (
                    self.config.orderbook_queue_threshold
                    if table_prefix == "orderbook"
                    else self.config.trade_queue_threshold
                )

                if queue_size >= threshold:
                    batch_size = min(threshold, queue_size)
                    table_name = f"{table_prefix}_{symbol}"

                    batch_data = []
                    for _ in range(batch_size):
                        item = await queue.get()
                        item["timestamp"] = unix_to_mysql_datetime(iso8601_to_unix(item["timestamp"]))
                        item["local_timestamp"] = unix_to_mysql_datetime(iso8601_to_unix(item["local_timestamp"]))
                        batch_data.append(tuple(item[col] for col in columns))

                    await self.db.insert_batch(table_name, batch_data, columns)

                    logging.info(f"[✅] Inserted {batch_size} rows into {table_name} ({table_prefix}). Remaining: {queue.qsize()}")

                    last_flush_time = time.time()

                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"[❌] Insert Error for {symbol}: {e}")


    async def process_order_book_queue(self, symbol: str, queue: asyncio.Queue):
        """Processes a single order book queue."""
        columns = ["exchange", "symbol", "timestamp", "local_timestamp"] + [
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
        """Processes a single trade queue."""
        columns = ["exchange", "symbol", "trade_id", "price", "amount", "side", "timestamp", "local_timestamp"]
        await self.process_queue(symbol, queue, "trade", columns)

    async def batch_insert_order_books(self):
        """Launches an async task for each order book queue to process all symbols concurrently."""
        tasks = [asyncio.create_task(self.process_order_book_queue(symbol, queue)) for symbol, queue in order_book_queues.items()]
        await asyncio.gather(*tasks)

    async def batch_insert_trades(self):
        """Launches an async task for each trade queue to process all symbols concurrently."""
        tasks = [asyncio.create_task(self.process_trade_queue(symbol, queue)) for symbol, queue in trade_queues.items()]
        await asyncio.gather(*tasks)

    async def shutdown(self):
        """Set shutdown event to stop processing."""
        logging.info("[!] Stopping queue processor...")
        self.shutdown_event.set()


# Main execution
# async def main():
#     """Main function to start the async database writer."""
#     config = Config()
#     db = MySQLDatabase(config)
#     await db.connect()

#     queue_processor = QueueProcessor(db, config)

#     try:
#         await asyncio.gather(queue_processor.batch_insert_order_books(), queue_processor.batch_insert_trades())
#     finally:
#         await queue_processor.shutdown()
#         await db.close()


# # Run the event loop
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logging.info("[!] KeyboardInter
