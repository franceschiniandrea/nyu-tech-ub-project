import psycopg2
from psycopg2 import sql
from crypto_hft.utils.config import Config
from itertools import product

config = Config()

DB_CONFIG = {
    'database': config.db_name, 
    'user': config.db_user,
    'password': config.db_password,
    'host': config.db_host,
    'port': config.db_port
}

# todo this should be pulled from the config
BASE_TICKERS = [
    "OP_USDT", "SC_USDT", "LDO_USDT", "SUI_USDT",
    "NEAR_USDT", "DASH_USDT", "ATOM_USDT", "STEEM_USDT",
    "UNI_USDT", "PEPE_USDT", "BNB_USDT", "LINK_USDT",
    "BTC_USDT", "ETH_USDT"
]

def create_order_book_table(symbol):
    table_name = f"orderbook_{symbol.upper().replace('-', '_')}"

    price_cols = []
    for i, side, col_type in product(
        range(10), # number of levels in the ob
        ['bid', 'ask'], # bid and ask columns
        ['sz', 'px'] # one column for the size, one for the price
    ):
        price_cols.append(f'{side}_{i}_{col_type} NUMERIC')

    return sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            {price_cols_fmt}
            timestamp TIMESTAMPTZ NOT NULL,
            local_timestamp TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {idx_exchange} ON {table} (exchange);
        CREATE INDEX IF NOT EXISTS {idx_symbol} ON {table} (symbol);
        CREATE INDEX IF NOT EXISTS {idx_timestamp} ON {table} (timestamp DESC);
    """).format(
        table=sql.Identifier(table_name),
        idx_exchange=sql.Identifier(f"idx_{table_name}_exchange"),
        idx_symbol=sql.Identifier(f"idx_{table_name}_symbol"),
        idx_timestamp=sql.Identifier(f"idx_{table_name}_timestamp"),
        price_cols_fmt=sql.SQL(', ').join(map(sql.SQL, price_cols))
    )

def create_trade_table(symbol):
    table_name = f"trade_{symbol.upper().replace('-', '_')}"

    return sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            trade_id TEXT UNIQUE NOT NULL,
            price NUMERIC NOT NULL,
            amount NUMERIC NOT NULL,
            side TEXT CHECK (side IN ('buy', 'sell')),
            timestamp TIMESTAMPTZ NOT NULL,
            local_timestamp TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {idx_exchange} ON {table} (exchange);
        CREATE INDEX IF NOT EXISTS {idx_symbol} ON {table} (symbol);
        CREATE INDEX IF NOT EXISTS {idx_timestamp} ON {table} (timestamp DESC);
    """).format(
        table=sql.Identifier(table_name),
        idx_exchange=sql.Identifier(f"idx_{table_name}_exchange"),
        idx_symbol=sql.Identifier(f"idx_{table_name}_symbol"),
        idx_timestamp=sql.Identifier(f"idx_{table_name}_timestamp")
    )

def setup_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        for symbol in BASE_TICKERS:
            cur.execute(create_order_book_table(symbol))
            cur.execute(create_trade_table(symbol))
            print(f"[+] Tables created: orderbook_{symbol.lower()} & trade_{symbol.lower()}")

        conn.commit()
        cur.close()
        conn.close()
        print("[✅] Database setup completed!")

    except Exception as e:
        print(f"[❌] Error setting up database: {e}")

if __name__ == "__main__":
    # setup_database()
    print(create_order_book_table('BTC-USDT'))