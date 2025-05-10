import os 
import streamlit as st
import psycopg2
import numpy as np

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
