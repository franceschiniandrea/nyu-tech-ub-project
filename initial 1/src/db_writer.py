import asyncio
import psycopg2
import psycopg2.extras
import logging
from psycopg2 import sql
from config import DB_CONFIG, BASE_TICKERS
from symbol_mapper import REVERSE_SYMBOL_MAP  # ✅ Import reverse symbol map

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("db_writer.log"),
        logging.StreamHandler()
    ]
)

order_book_queues = {symbol.lower().replace("_", ""): asyncio.Queue() for symbol in BASE_TICKERS}
trade_queues = {symbol.lower().replace("_", ""): asyncio.Queue() for symbol in BASE_TICKERS}

def connect_db():
    """Establishes a connection to PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)

def format_table_name(base_symbol, is_trade=False):
    """
    Formats table names to match PostgreSQL schema.
    """
    formatted_symbol = base_symbol.lower().replace("_", "")  
    return f"trade{formatted_symbol}" if is_trade else f"orderbook{formatted_symbol}"

def insert_batch_order_books(conn, received_symbol, batch):
    """Inserts a batch of already-structured order book data into PostgreSQL."""
    original_symbol = REVERSE_SYMBOL_MAP.get(received_symbol, received_symbol)
    table_name = format_table_name(original_symbol, is_trade=False)
    
    query = sql.SQL("""
        INSERT INTO {} (exchange, bid0, bid0_size, bid1, bid1_size, bid2, bid2_size, 
                        bid3, bid3_size, bid4, bid4_size, bid5, bid5_size, bid6, bid6_size, 
                        bid7, bid7_size, bid8, bid8_size, bid9, bid9_size,
                        ask0, ask0_size, ask1, ask1_size, ask2, ask2_size, ask3, ask3_size, 
                        ask4, ask4_size, ask5, ask5_size, ask6, ask6_size, ask7, ask7_size, 
                        ask8, ask8_size, ask9, ask9_size, timestamp, local_timestamp)
        VALUES %s
    """).format(sql.Identifier(table_name))
    
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, batch)
        conn.commit()
        logging.info(f"[DB INSERT] Inserted {len(batch)} order book entries into {table_name}.")

def insert_batch_trades(conn, received_symbol, batch):
    """Inserts a batch of trade data into PostgreSQL."""
    original_symbol = REVERSE_SYMBOL_MAP.get(received_symbol, received_symbol)
    table_name = format_table_name(original_symbol, is_trade=True)
    
    query = sql.SQL("""
        INSERT INTO {} (exchange, trade_id, price, amount, side, timestamp, local_timestamp)
        VALUES %s
    """).format(sql.Identifier(table_name))
    
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, batch)
        conn.commit()
        logging.info(f"[DB INSERT] Inserted {len(batch)} trades into {table_name}.")

async def batch_insert_order_books():
    """Continuously inserts order book data when queue reaches 5 entries."""
    conn = connect_db()
    while True:
        for received_symbol, queue in order_book_queues.items():
            if queue.qsize() >= 5:
                batch = [await queue.get() for _ in range(5)]
                insert_batch_order_books(conn, received_symbol, batch)

async def batch_insert_trades():
    """Continuously inserts trade data when queue reaches 5 entries."""
    conn = connect_db()
    while True:
        for received_symbol, queue in trade_queues.items():
            if queue.qsize() >= 5:
                batch = [await queue.get() for _ in range(5)]
                insert_batch_trades(conn, received_symbol, batch)

# async def main():
#     """Runs batch insert functions for order books and trades."""
#     logging.info("[+] Starting database writer...")
#     await asyncio.gather(batch_insert_order_books(), batch_insert_trades())

# if __name__ == "__main__":
#     asyncio.run(main()) okay how is my script for db_writer?