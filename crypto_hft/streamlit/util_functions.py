import os 
import streamlit as st
import psycopg2
import numpy as np
import datetime
from loguru import logger
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

@st.cache_data()
def fetch_available_symbols(): 
    db_config = {
        'database': os.getenv('POSTGRES_DATABASE'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
    }

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
            """)
            tables = cur.fetchall()
    
    table_names = [t[0] for t in tables if 'orderbook' in t[0] or 'trade' in t[0]]
    symbols = np.unique([t.replace('orderbook_', '').replace('trade_', '') for t in table_names])
    return list(symbols)

@st.cache_data()
def fetch_data_from_db(_engine, symbol: str, from_date: datetime.date, to_date: datetime.date): 
    logger.debug(f'requested data for symbol {symbol} for dates {from_date} - {to_date}', symbol)

    query = f"""
        SELECT * FROM trade_ada_usdt
        WHERE timestamp BETWEEN '{from_date}' AND '{to_date}'
    """

    with ThreadPoolExecutor() as executor:
        future = executor.submit(pd.read_sql, query, _engine)
        df = future.result()

    logger.debug(f'Fetched {len(df)} rows from the database for symbol {symbol}.\n{df.head()}')
    return df

@st.cache_resource()
def get_db_engine(): 
    try:
        # Get database credentials from environment variables
        host = os.getenv('POSTGRES_HOST')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        database = os.getenv('POSTGRES_DATABASE')
        
        # Create the connection string
        conn_string = f"postgresql://{user}:{password}@{host}/{database}"
        
        # Create the engine
        engine = create_engine(conn_string)
        
        return engine
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None
