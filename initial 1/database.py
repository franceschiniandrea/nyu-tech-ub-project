import psycopg2
from psycopg2 import sql

DB_PARAMS = {
    "database": "tardis_db",
    "user": "tardis_user",
    "password": "unsecuredpassword",
    "host": "localhost",
    "port": 5432
}

base_tickers = [
    "OP_USDT",  #Optimism (OP)	
    "SC_USDT", #Siacoin (SC)	
    "LDO_USDT", #Lido DAO (LDO)	
    "SUI_USDT", #Sui (SUI)	
    "NEAR_USDT", #NEAR Protocol (NEAR)	
    "DASH_USDT", #Dash (DASH)	
    "ATOM_USDT", #Cosmos (ATOM)	
    "STEEM_USDT", #Steem (STEEM)	
    "UNI_USDT", #Uniswap (UNI)	
    "PEPE_USDT", #Pepe (PEPE)	
    "BNB_USDT", #Binance Coin (BNB)	
    "LINK_USDT", #Chainlink (LINK)	
    "BTC_USDT", #Bitcoin (BTC)
    "ETH_USDT" #Ethereum (ETH)
]

def create_order_book_table(symbol):
    table_name = f"order_book_{symbol.lower()}".replace("_", "")

    create_table_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_order_book_exchange ON {} (exchange);
        CREATE INDEX IF NOT EXISTS idx_order_book_timestamp ON {} (timestamp DESC);
        """
    ).format(sql.Identifier(table_name), sql.Identifier(table_name), sql.Identifier(table_name))

    return create_table_query

def create_trade_table(symbol):
    table_name = f"trade_{symbol.lower()}".replace("_", "")

    create_table_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            price NUMERIC NOT NULL,
            amount NUMERIC NOT NULL,
            side TEXT CHECK (side IN ('buy', 'sell')),
            timestamp TIMESTAMPTZ NOT NULL,
            local_timestamp TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trade_exchange ON {} (exchange);
        CREATE INDEX IF NOT EXISTS idx_trade_timestamp ON {} (timestamp DESC);
        """
    ).format(sql.Identifier(table_name), sql.Identifier(table_name), sql.Identifier(table_name))

    return create_table_query

def setup_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        for symbol in base_tickers:
            cur.execute(create_order_book_table(symbol))
            cur.execute(create_trade_table(symbol))
            print(f"[+] Tables created (if not exists): order_book_{symbol.lower()} & trade_{symbol.lower()}")

        conn.commit()
        cur.close()
        conn.close()
        print("[✅] Database setup completed!")

    except Exception as e:
        print(f"[❌] Error setting up database: {e}")

if __name__ == "__main__":
    setup_database()
