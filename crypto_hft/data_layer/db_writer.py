import asyncio
import logging
import time
import io
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from crypto_hft.utils.config import Config

config = Config()
DB_CONFIG = {
    'database': config.db_name, 
    'user': config.db_user,
    'password': config.db_password,
    'host': config.db_host,
    'port': config.db_port
}

order_book_queues: dict[str, asyncio.Queue] = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in config.base_tickers}
trade_queues: dict[str, asyncio.Queue] = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in config.base_tickers}

# Create connection pool (min 2, max 10 connections)
DB_POOL = ThreadedConnectionPool(2, 10, **DB_CONFIG)

def get_db_connection():
    """Fetch a connection from the pool instead of opening a new one each time."""
    return DB_POOL.getconn()

def release_db_connection(conn):
    """Release the connection back to the pool."""
    DB_POOL.putconn(conn)

def format_table_name(base_symbol, is_trade=False):
    """Formats the table name to match PostgreSQL schema."""
    formatted_symbol = base_symbol.upper().replace("-", "_")  
    return f"trade_{formatted_symbol}" if is_trade else f"orderbook_{formatted_symbol}"

async def batch_insert_order_books():
    """Directly inserts data from the queue to PostgreSQL when it reaches the threshold."""
    while True:
        conn = get_db_connection()
        try:
            for received_symbol, queue in order_book_queues.items():
                queue_size = queue.qsize()

                if queue_size >= 1000:
                    logging.info(f"[BATCH TRIGGERED] {received_symbol} | Queue Size: {queue_size}")

                    start_time = time.perf_counter()  
                    batch = [await queue.get() for _ in range(1000)]  
                    insert_batch_order_books(conn, received_symbol, batch)
                    end_time = time.perf_counter()
                    
                    latency = (end_time - start_time) * 1000  # Convert to ms
                    logging.info(f"[LATENCY] Order Book | {received_symbol} | {latency:.2f} ms")

                if queue_size > 5000:
                    logging.warning(f"[WARNING] {received_symbol} order book queue is too large: {queue_size}")

            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"[ORDER BOOK ERROR] {e}")
        finally:
            release_db_connection(conn)

async def batch_insert_trades():
    """Directly inserts trade data from queue to PostgreSQL when it reaches the threshold."""
    while True:
        conn = get_db_connection()
        try:
            for received_symbol, queue in trade_queues.items():
                queue_size = queue.qsize()

                if queue_size >= 1000:
                    logging.info(f"[TRADE BATCH TRIGGERED] {received_symbol} | Queue Size: {queue_size}")

                    start_time = time.perf_counter()  # ✅ Start latency timing
                    batch = [await queue.get() for _ in range(1000)]  # ✅ Directly extract from queue
                    insert_batch_trades(conn, received_symbol, batch)
                    end_time = time.perf_counter()
                    
                    latency = (end_time - start_time) * 1000  # Convert to ms
                    logging.info(f"[LATENCY] Trade | {received_symbol} | {latency:.2f} ms")

                if queue_size > 5000:
                    logging.warning(f"[WARNING] {received_symbol} trade queue is too large: {queue_size}")

            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"[TRADE ERROR] {e}")
        finally:
            release_db_connection(conn)

def insert_batch_order_books(conn, received_symbol, batch):
    """Inserts order book data using COPY for fast batch writes."""
    table_name = format_table_name(received_symbol, is_trade=False)

    output = io.StringIO()
    for item in batch:
        output.write(f"{item['exchange']},{item['symbol']},{item['bid_0_px']},{item['bid_0_sz']},"
                     f"{item['ask_0_px']},{item['ask_0_sz']},{item['timestamp']},{item['local_timestamp']}\n")
    output.seek(0)

    try:
        start_time = time.perf_counter()  # ✅ Start timing
        with conn.cursor() as cur:
            cur.copy_from(output, table_name, sep=",", columns=(
                "exchange", "symbol", "bid_0_px", "bid_0_sz", 
                "ask_0_px", "ask_0_sz", "timestamp", "local_timestamp"
            ))
            conn.commit()
        end_time = time.perf_counter()
        
        db_latency = (end_time - start_time) * 1000  # Convert to ms
        logging.info(f"[DB LATENCY] Order Book | {received_symbol} | COPY Latency: {db_latency:.2f} ms")

    except Exception as e:
        logging.error(f"[DB ERROR] Order Book | {received_symbol} | COPY failed: {e}")

def insert_batch_trades(conn, received_symbol, batch):
    """Inserts trade data using COPY for fast batch writes."""
    table_name = format_table_name(received_symbol, is_trade=True)

    output = io.StringIO()
    for item in batch:
        output.write(f"{item['exchange']},{item['symbol']},{item['trade_id']},{item['price']},"
                     f"{item['amount']},{item['side']},{item['timestamp']},{item['local_timestamp']}\n")
    output.seek(0)

    try:
        start_time = time.perf_counter()
        with conn.cursor() as cur:
            cur.copy_from(output, table_name, sep=",", columns=(
                "exchange", "symbol", "trade_id", "price", 
                "amount", "side", "timestamp", "local_timestamp"
            ))
            conn.commit()
        end_time = time.perf_counter()
        
        db_latency = (end_time - start_time) * 1000  # Convert to ms
        logging.info(f"[DB LATENCY] Trade | {received_symbol} | COPY Latency: {db_latency:.2f} ms")

    except Exception as e:
        logging.error(f"[DB ERROR] Trade | {received_symbol} | COPY failed: {e}")

async def main():
    """Runs batch insert functions for order books and trades."""
    logging.info("[+] Starting database writer...")
    await asyncio.gather(batch_insert_order_books(), batch_insert_trades())

if __name__ == "__main__":
    asyncio.run(main())


