import asyncio
import logging
import time
import aiomysql
from crypto_hft.utils.config import Config
from crypto_hft.data_layer.queue_manager import order_book_queues,trade_queues
import datetime
import ciso8601

# Load configuration
config = Config()

# Async shutdown event
shutdown_event = asyncio.Event()


class MySQLDatabase:
    """Handles async MySQL connections and batch inserts."""

    def __init__(self):
        self.pool = None

    async def connect(self):
        """Create a connection pool for MySQL."""
        self.pool = await aiomysql.create_pool(
            host=config.db_host,
            user=config.db_user,
            password=config.db_password,
            db=config.db_database,
            port=config.db_port,
            autocommit=True
        )
        logging.info("✅ Async MySQL Connection Established")

    async def insert_batch(self, table_name, batch_data, columns):
        """Insert data asynchronously using aiomysql."""
        if not batch_data:
            return

        async with self.pool.acquire() as conn:  # ✅ Get a connection from the pool
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


def iso8601_to_unix(timestamp: str) -> float:
    """Convert ISO 8601 formatted timestamp to Unix timestamp (seconds since epoch)."""
    dt = ciso8601.parse_datetime(timestamp)
    return dt.timestamp()  # Returns float with microsecond precision

def unix_to_mysql_datetime(unix_time: float) -> str:
    """Convert Unix timestamp to MySQL DATETIME(6) format: 'YYYY-MM-DD HH:MM:SS.mmmmmm'."""
    dt = datetime.datetime.utcfromtimestamp(unix_time)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{dt.microsecond:06d}"

async def process_order_book_queue(symbol, queue, db):
    """Processes a single order book queue continuously."""
    last_flush_time = time.time()

    while not shutdown_event.is_set():
        try:
            queue_size = queue.qsize()
            elapsed_time = time.time() - last_flush_time  # Time since last insert

            # Process if queue has 10,000 messages OR 30 seconds have passed
            if queue_size >= 10_000 or (elapsed_time > 30 and queue_size > 0):
                batch_size = min(10_000, queue_size)
                table_name = f"orderbook_{symbol.upper()}"

                columns = ["exchange", "symbol", "timestamp", "local_timestamp"] + [
                    f"bid_{i}_sz" for i in range(config.orderbook_levels)
                ] + [
                    f"bid_{i}_px" for i in range(config.orderbook_levels)
                ] + [
                    f"ask_{i}_sz" for i in range(config.orderbook_levels)
                ] + [
                    f"ask_{i}_px" for i in range(config.orderbook_levels)
                ]

                # Convert timestamp and local_timestamp, then prepare batch data
                batch_data = []
                for _ in range(batch_size):
                    item = await queue.get()  # Get next item from the queue
                    # Convert both 'timestamp' and 'local_timestamp' to MySQL format
                    item["timestamp"] = unix_to_mysql_datetime(iso8601_to_unix(item["timestamp"]))
                    item["local_timestamp"] = unix_to_mysql_datetime(iso8601_to_unix(item["local_timestamp"]))
                    batch_data.append(tuple(item[col] for col in columns))

                # Insert batch into MySQL
                await db.insert_batch(table_name, batch_data, columns)

                logging.info(f"[✅] Inserted {batch_size} rows for {symbol}. Remaining: {queue.qsize()}")

                last_flush_time = time.time()

            await asyncio.sleep(0.1)  # Prevent excessive CPU usage
        except Exception as e:
            logging.error(f"[❌] Order Book Insert Error for {symbol}: {e}")


async def batch_insert_order_books(db):
    """Launches an async task for each order book queue to process all symbols concurrently."""
    tasks = [asyncio.create_task(process_order_book_queue(symbol, queue, db)) for symbol, queue in order_book_queues.items()]
    await asyncio.gather(*tasks)  # ✅ Runs all queue processing in parallel


async def main():
    """Main function to start the async database writer."""
    db = MySQLDatabase()
    await db.connect()

    try:
        await batch_insert_order_books(db)
    finally:
        await db.close()


# Run the event loop
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("[!] KeyboardInterrupt received. Shutting down...")
        shutdown_event.set()
