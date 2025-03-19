# This script sets up the database and creates tables for order books and trades.
# It also creates indexes for efficient querying.
from itertools import product
from crypto_hft.utils.config import Config
import mysql.connector

# Load configuration
config = Config()

# MySQL connection configuration
DB_CONFIG = {
    'database': config.db_database,
    'user': config.db_user,
    'password': config.db_password,
    'host': config.db_host,
    'port': config.db_port,  
    'charset': 'utf8', 
}

BASE_TICKERS = config.base_tickers  # Access through the instance

from itertools import product

def create_order_book_table(symbol):
    table_name = f"orderbook_{symbol.upper().replace('-', '_')}"

    price_cols = []
    for i, side, col_type in product(range(15), ['bid', 'ask'], ['sz', 'px']):
        price_cols.append(f'{side}_{i}_{col_type} DECIMAL(20,10) NULL')

    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            exchange VARCHAR(255) NOT NULL,
            symbol VARCHAR(255) NOT NULL,
            timestamp DATETIME(6) NOT NULL,
            local_timestamp DATETIME(6) NOT NULL, 
            {', '.join(price_cols)}
        );
    """
    return query


def create_trade_table(symbol):
    table_name = f"trade_{symbol.upper().replace('-', '_')}"

    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            exchange VARCHAR(255) NOT NULL,
            symbol VARCHAR(255) NOT NULL,
            trade_id VARCHAR(255) UNIQUE NOT NULL,
            price DECIMAL(20,10) NOT NULL,  
            amount DECIMAL(20,10) NOT NULL, 
            side VARCHAR(255) NOT NULL,
            timestamp DATETIME(6) NOT NULL,
            local_timestamp DATETIME(6) NOT NULL
        );
    """
    return query


def setup_database():
    try:
        # Connect to MySQL using mysql-connector
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Loop through all symbols to create tables
        for symbol in BASE_TICKERS:
            # Create order book table
            cursor.execute(create_order_book_table(symbol))
            conn.commit()  

            # Create trade table
            cursor.execute(create_trade_table(symbol))
            conn.commit() 

            print(f"[+] Tables created for {symbol}")

        cursor.close()
        conn.close()  
        print("[✅] Database setup completed!")

    except mysql.connector.Error as e:
        print(f"[❌] Error setting up database: {e}")

if __name__ == "__main__":
    setup_database() 
