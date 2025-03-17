import asyncio
import psycopg2
import psycopg2.extras
import logging
import time
import io
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
from config import DB_CONFIG, BASE_TICKERS
from symbol_mapper import REVERSE_SYMBOL_MAP  
from utils import setup_logging

setup_logging("database_writer.log")

order_book_queues = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in BASE_TICKERS}
trade_queues = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in BASE_TICKERS}

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
        output.write(f"{item['exchange']},{item['symbol']},{item['bid0']},{item['bid0_size']},"
                     f"{item['ask0']},{item['ask0_size']},{item['timestamp']},{item['local_timestamp']}\n")
    output.seek(0)

    try:
        start_time = time.perf_counter()  # ✅ Start timing
        with conn.cursor() as cur:
            cur.copy_from(output, table_name, sep=",", columns=(
                "exchange", "symbol", "bid0", "bid0_size", 
                "ask0", "ask0_size", "timestamp", "local_timestamp"
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






# import asyncio
# import psycopg2
# import psycopg2.extras
# import logging
# import time
# from psycopg2 import sql
# from config import DB_CONFIG, BASE_TICKERS
# from symbol_mapper import REVERSE_SYMBOL_MAP  

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("db_writer.log"),
#         logging.StreamHandler()
#     ]
# )

# # Create asyncio queues for order books and trades
# order_book_queues = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in BASE_TICKERS}
# trade_queues = {symbol.upper().replace("-", "_"): asyncio.Queue() for symbol in BASE_TICKERS}


# def connect_db():
#     """Establishes a connection to PostgreSQL."""
#     return psycopg2.connect(**DB_CONFIG)


# def format_table_name(base_symbol, is_trade=False):
#     """Formats table names to match PostgreSQL schema."""
#     formatted_symbol = base_symbol.upper().replace("-", "_")  # Preserve underscores
#     return f"trade_{formatted_symbol}" if is_trade else f"orderbook_{formatted_symbol}"


# def insert_batch_order_books(conn, exchange, received_symbol, batch):
#     """Inserts a batch of structured order book data into PostgreSQL."""
#     original_symbol = REVERSE_SYMBOL_MAP.get(exchange, {}).get(received_symbol, received_symbol)
#     table_name = format_table_name(original_symbol, is_trade=False)

#     query = sql.SQL("""
#         INSERT INTO {} (exchange, symbol, bid0, bid0_size, bid1, bid1_size, bid2, bid2_size, 
#                         bid3, bid3_size, bid4, bid4_size, bid5, bid5_size, bid6, bid6_size, 
#                         bid7, bid7_size, bid8, bid8_size, bid9, bid9_size,
#                         ask0, ask0_size, ask1, ask1_size, ask2, ask2_size, ask3, ask3_size, 
#                         ask4, ask4_size, ask5, ask5_size, ask6, ask6_size, ask7, ask7_size, 
#                         ask8, ask8_size, ask9, ask9_size, timestamp, local_timestamp)
#         VALUES %s
#     """).format(sql.Identifier(table_name))

#     try:
#         with conn.cursor() as cur:
#             psycopg2.extras.execute_values(cur, query, batch)
#             conn.commit()
#             logging.info(f"[DB SUCCESS] {received_symbol} | Inserted {len(batch)} order book entries into {table_name}.")
#     except Exception as e:
#         logging.error(f"[DB ERROR] {received_symbol} | Failed to insert batch: {e}")


# def insert_batch_trades(conn, exchange, received_symbol, batch):
#     """Inserts a batch of trade data into PostgreSQL."""
#     original_symbol = REVERSE_SYMBOL_MAP.get(exchange, {}).get(received_symbol, received_symbol)
#     table_name = format_table_name(original_symbol, is_trade=True)

#     query = sql.SQL("""
#         INSERT INTO {} (exchange, symbol, trade_id, price, amount, side, timestamp, local_timestamp)
#         VALUES %s
#     """).format(sql.Identifier(table_name))

#     try:
#         with conn.cursor() as cur:
#             psycopg2.extras.execute_values(cur, query, batch)
#             conn.commit()
#             logging.info(f"[DB SUCCESS] {received_symbol} | Inserted {len(batch)} trades into {table_name}.")
#     except Exception as e:
#         logging.error(f"[DB ERROR] {received_symbol} | Failed to insert batch: {e}")


# async def batch_insert_order_books():
#     conn = connect_db()
#     while True:
#         try:
#             for received_symbol, queue in order_book_queues.items():
#                 queue_size = queue.qsize()

#                 if queue_size >= 5:
#                     logging.info(f"[BATCH TRIGGERED] {received_symbol} | Preparing batch insert")

#                     batch = []
#                     for _ in range(min(queue_size, 1000)):
#                         try:
#                             item = queue.get_nowait()
#                             batch.append((
#                                 item["exchange"], item["symbol"], item["bid0"], item["bid0_size"],
#                                 item["bid1"], item["bid1_size"], item["bid2"], item["bid2_size"],
#                                 item["bid3"], item["bid3_size"], item["bid4"], item["bid4_size"],
#                                 item["bid5"], item["bid5_size"], item["bid6"], item["bid6_size"],
#                                 item["bid7"], item["bid7_size"], item["bid8"], item["bid8_size"],
#                                 item["bid9"], item["bid9_size"], item["ask0"], item["ask0_size"],
#                                 item["ask1"], item["ask1_size"], item["ask2"], item["ask2_size"],
#                                 item["ask3"], item["ask3_size"], item["ask4"], item["ask4_size"],
#                                 item["ask5"], item["ask5_size"], item["ask6"], item["ask6_size"],
#                                 item["ask7"], item["ask7_size"], item["ask8"], item["ask8_size"],
#                                 item["ask9"], item["ask9_size"], item["timestamp"], item["local_timestamp"]
#                             ))
#                         except asyncio.QueueEmpty:
#                             break

#                     if batch:
#                         insert_batch_order_books(conn, item["exchange"], received_symbol, batch)

#             await asyncio.sleep(1)
#         except Exception as e:
#             logging.error(f"[ORDER BOOK ERROR] {e}")
#             await asyncio.sleep(1)


# async def batch_insert_trades():
#     """Continuously inserts trade data when queue reaches 5 entries."""
#     conn = connect_db()
#     while True:
#         try:
#             for received_symbol, queue in trade_queues.items():
#                 queue_size = queue.qsize()

#                 if queue_size >= 1000:
#                     logging.info(f"[TRADE BATCH TRIGGERED] {received_symbol} | Preparing batch insert")

#                     batch = []
#                     for _ in range(min(queue_size, 5)):  
#                         try:
#                             item = queue.get_nowait()
#                             batch.append((
#                                 item["exchange"], item["symbol"], item["trade_id"], item["price"], 
#                                 item["amount"], item["side"], item["timestamp"], item["local_timestamp"]
#                             ))
#                         except asyncio.QueueEmpty:
#                             break

#                     if batch:
#                         insert_batch_trades(conn, item["exchange"], received_symbol, batch)

#             await asyncio.sleep(1)
#         except Exception as e:
#             logging.error(f"[TRADE ERROR] {e}")
#             await asyncio.sleep(1)


# # async def main():
# #     """Runs batch insert functions for order books and trades."""
# #     logging.info("[+] Starting database writer...")
# #     await asyncio.gather(batch_insert_order_books(), batch_insert_trades())


# # if __name__ == "__main__":
# #     asyncio.run(main())
