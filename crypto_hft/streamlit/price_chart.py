import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlalchemy
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path
import sys
import time

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# Import the Config class from crypto_hft
from crypto_hft.utils.config import Config

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / "utils" / ".env")

# Set up the page
st.set_page_config(
    page_title="Crypto Price Chart",
    page_icon="📈",
    layout="wide"
)

# Page title
st.title("Crypto Price Chart")

# Sidebar controls
st.sidebar.header("Chart Controls")

# Trading pair selection
trading_pair = st.sidebar.selectbox(
    "Select Trading Pair",
    ["BTC/USDT", "SOL/USDT"],
    index=0
)

# Auto-refresh settings
auto_refresh = st.sidebar.checkbox("Auto-refresh data", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 1, 60, 5)

if auto_refresh:
    st.write(f"Auto-refreshing every {refresh_interval} seconds")
    time_placeholder = st.empty()

# Create a connection to the PostgreSQL database
def get_db_connection():
    try:
        # Get database credentials from environment variables
        host = os.getenv('postgres_host')
        user = os.getenv('postgres_user')
        password = os.getenv('postgres_password')
        database = os.getenv('postgres_database')
        
        # Create the connection string
        conn_string = f"postgresql://{user}:{password}@{host}/{database}"
        
        # Create the engine
        engine = create_engine(conn_string)
        
        return engine
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Function to fetch trade data from the database
@st.cache_data(ttl=60)  # Cache the data for 60 seconds
def fetch_trade_data(_engine, trading_pair, limit=10000):
    try:
        # Determine which table to query based on the trading pair
        if trading_pair == "BTC/USDT":
            table_name = "trade_btc_usdt"
            base_currency = "BTC"
        elif trading_pair == "SOL/USDT":
            table_name = "trade_sol_usdt"
            base_currency = "SOL"
        else:
            st.error(f"Unknown trading pair: {trading_pair}")
            return pd.DataFrame()
        
        # Use the correct columns from the table
        query = f"""
        SELECT 
            id,
            exchange,
            trade_id,
            price, 
            amount, 
            side, 
            timestamp,
            local_timestamp
        FROM 
            {table_name}
        ORDER BY 
            local_timestamp DESC
        LIMIT {limit}
        """
        
        # Execute the query and load into a pandas DataFrame
        df = pd.read_sql(query, _engine)
        
        # Convert timestamp columns to datetime if they're not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['local_timestamp'] = pd.to_datetime(df['local_timestamp'])
        
        # Sort by local_timestamp in ascending order for the chart
        df = df.sort_values('local_timestamp')
        
        return df, base_currency
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame(), ""

# Function to create a candlestick chart
def create_candlestick_chart(df, base_currency, timeframe='1min'):
    # Resample the data to the specified timeframe
    ohlc = df.set_index('local_timestamp').price.resample(timeframe).ohlc()
    
    # Create a candlestick chart
    fig = go.Figure(data=[go.Candlestick(
        x=ohlc.index,
        open=ohlc['open'],
        high=ohlc['high'],
        low=ohlc['low'],
        close=ohlc['close'],
        name=f'{base_currency}/USDT'
    )])
    
    # Update layout
    fig.update_layout(
        title=f'{base_currency}/USDT Price',
        xaxis_title='Time',
        yaxis_title='Price (USDT)',
        xaxis_rangeslider_visible=False,
        height=600
    )
    
    return fig

# Function to create a line chart of trades
def create_trade_chart(df, base_currency):
    # Create a line chart
    fig = go.Figure()
    
    # Add the price line
    fig.add_trace(go.Scatter(
        x=df['local_timestamp'],
        y=df['price'],
        mode='lines',
        name='Price'
    ))
    
    # Add buy trades as green markers
    buys = df[df['side'] == 'buy']
    if not buys.empty:
        fig.add_trace(go.Scatter(
            x=buys['local_timestamp'],
            y=buys['price'],
            mode='markers',
            marker=dict(color='green', size=buys['amount']/buys['amount'].max()*10+2),
            name='Buy'
        ))
    
    # Add sell trades as red markers
    sells = df[df['side'] == 'sell']
    if not sells.empty:
        fig.add_trace(go.Scatter(
            x=sells['local_timestamp'],
            y=sells['price'],
            mode='markers',
            marker=dict(color='red', size=sells['amount']/sells['amount'].max()*10+2),
            name='Sell'
        ))
    
    # Update layout
    fig.update_layout(
        title=f'{base_currency}/USDT Trades',
        xaxis_title='Time',
        yaxis_title='Price (USDT)',
        height=600
    )
    
    return fig

# Main app
def main():
    # Connect to the database
    engine = get_db_connection()
    
    if engine is None:
        st.warning("Please check your database credentials in the .env file.")
        return
    
    # Number of trades to fetch
    num_trades = st.sidebar.slider("Number of trades to display", 100, 10000, 5000)
    
    # Chart type selection
    chart_type = st.sidebar.selectbox(
        "Select chart type",
        ["Candlestick", "Trade Line"]
    )
    
    # Timeframe selection for candlestick chart
    timeframe = st.sidebar.selectbox(
        "Select timeframe for candlestick chart",
        ["1min", "5min", "15min", "30min", "1h", "4h", "1d"],
        index=1
    )
    
    # Fetch the data
    with st.spinner("Fetching trade data..."):
        df, base_currency = fetch_trade_data(engine, trading_pair, num_trades)
    
    if df.empty:
        st.warning("No data available. Please check your database connection and table name.")
        return
    
    # Display some statistics
    st.subheader("Trade Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Latest Price", f"${df['price'].iloc[-1]:.2f}")
    
    with col2:
        price_change = df['price'].iloc[-1] - df['price'].iloc[0]
        st.metric("Price Change", f"${price_change:.2f}", delta=f"{price_change:.2f}")
    
    with col3:
        st.metric("Number of Trades", len(df))
    
    with col4:
        timespan = df['local_timestamp'].max() - df['local_timestamp'].min()
        st.metric("Time Span", f"{timespan}")
    
    # Display the selected chart
    if chart_type == "Candlestick":
        st.plotly_chart(create_candlestick_chart(df, base_currency, timeframe), use_container_width=True)
    else:
        st.plotly_chart(create_trade_chart(df, base_currency), use_container_width=True)
    
    # Display the raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
