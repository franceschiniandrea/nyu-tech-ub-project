import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "database": "tardis_db",
    "user": "tardis_user",
    "password": "unsecuredpassword",
    "host": "localhost",
    "port": 5432
}

BASE_TICKERS = [
    "OP_USDT", "SC_USDT", "LDO_USDT", "SUI_USDT",
    "NEAR_USDT", "DASH_USDT", "ATOM_USDT", "STEEM_USDT",
    "UNI_USDT", "PEPE_USDT", "BNB_USDT", "LINK_USDT",
    "BTC_USDT", "ETH_USDT"
]

def create_order_book_table(symbol):
    table_name = f"orderbook_{symbol.upper().replace('-', '_')}"
    
    return sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            bid0 NUMERIC, bid0_size NUMERIC,
            bid1 NUMERIC, bid1_size NUMERIC,
            bid2 NUMERIC, bid2_size NUMERIC,
            bid3 NUMERIC, bid3_size NUMERIC,
            bid4 NUMERIC, bid4_size NUMERIC,
            bid5 NUMERIC, bid5_size NUMERIC,
            bid6 NUMERIC, bid6_size NUMERIC,
            bid7 NUMERIC, bid7_size NUMERIC,
            bid8 NUMERIC, bid8_size NUMERIC,
            bid9 NUMERIC, bid9_size NUMERIC,
            ask0 NUMERIC, ask0_size NUMERIC,
            ask1 NUMERIC, ask1_size NUMERIC,
            ask2 NUMERIC, ask2_size NUMERIC,
            ask3 NUMERIC, ask3_size NUMERIC,
            ask4 NUMERIC, ask4_size NUMERIC,
            ask5 NUMERIC, ask5_size NUMERIC,
            ask6 NUMERIC, ask6_size NUMERIC,
            ask7 NUMERIC, ask7_size NUMERIC,
            ask8 NUMERIC, ask8_size NUMERIC,
            ask9 NUMERIC, ask9_size NUMERIC,
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
    setup_database()
