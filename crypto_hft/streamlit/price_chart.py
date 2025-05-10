import streamlit as st
import pandas as pd
import plotly.graph_objects as go # type: ignore
import sqlalchemy
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path
import numpy as np
import psycopg2
import sys
import time
from streamlit.delta_generator import DeltaGenerator
import asyncio
import msgspec
import websockets
from loguru import logger
import uuid
from crypto_hft.streamlit.util_functions import fetch_available_symbols
from crypto_hft.utils.config import Config

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / "utils" / ".env")

# Set up the page
st.set_page_config(
    page_title="Crypto Price Chart",
    page_icon="📈",
    layout="wide"
)

available_symbols = fetch_available_symbols()

# Create a connection to the PostgreSQL database
def get_db_connection():
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

# Page title
st.title("Crypto Price Chart")
# Sidebar controls
st.sidebar.header("Chart Controls")
st.subheader("Trade Statistics")

trading_pair: DeltaGenerator = st.sidebar.selectbox(
    "Select Trading Pair",
    available_symbols,
    index=0,
    format_func=lambda s: s.upper().replace('_', '/'),
)

# Function to fetch trade data from the database
@st.cache_data(ttl=60)  # Cache the data for 60 seconds
def fetch_trade_data(_engine, trading_pair, limit=10000):
    try:
        table_name = f'trade_{trading_pair}'
        base_currency = (
            trading_pair
            .replace('perps_','')
            .split('_')[0]
            .replace('usdt', '')
            .upper()
        )
        
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

class Dashboard(): 
    def __init__(self) -> None: 
        self.trades: list[dict] = []
        self.order_book: list[dict] = []

        self.price_chart = None
        self.data_container: DeltaGenerator | None = None
        self.symbol_changed = asyncio.Event()

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)

    async def fetch_from_websocket(self) -> None: 
        while True: 
            uri = f'ws://localhost:9999/{trading_pair}'
            decoder = msgspec.json.Decoder()
            logger.info(f"Connecting to {uri}...")
            async with websockets.connect(f'ws://localhost:9999/{trading_pair}') as ws: 
                logger.info(f"Connected to WebSocket for {trading_pair}.")
                try:
                    async for msg in ws: 
                        data = decoder.decode(msg)
                        # print(f"Received message: {data}")

                        if 'trade_id' in data:
                            self.trades.append(data)
                        elif 'bid_0_px' in data: 
                            self.order_book.append(data)
                        else:
                            raise ValueError("Invalid message format: 'trade_id' not found in data")
                        
                except websockets.ConnectionClosed:
                    logger.error("Connection closed, attempting to reconnect...")

    def first_render(self) -> None: 
        self.data_container = st.empty()

    def render(self): 
        if len(self.trades) == 0:
            with self.data_container.container():
                st.warning("No data available. Please check your database connection and table name.")
            
            return

        print(f'selected trading pair: {trading_pair}')
        df = pd.DataFrame(self.trades)  
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['local_timestamp'] = pd.to_datetime(df['timestamp'])
        base_currency = 'btc_usdt'
        timeframe = '1min'
        chart_type = 'Candlestick'

        # print(f"Received trade data: {df.head(2)}")
        # Display some statistics
        
        with self.data_container.container():
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Latest Price", f"${df['price'].iloc[-1]:.2f}")
            
            with col2:
                price_change = df['price'].iloc[-1] - df['price'].iloc[0]
                st.metric("Price Change", f"${price_change:.2f}", delta=f"{price_change:.2f}")
            
            with col3:
                st.metric("Number of Trades", len(df))
            
            with col4:
                # timespan = df['local_timestamp'].max() - df['local_timestamp'].min()
                timespan = '1min'
                st.metric("Time Span", f"{timespan}")
            
            # Display the selected chart
            if chart_type == "Candlestick":
                st.plotly_chart(create_candlestick_chart(df, base_currency, timeframe), use_container_width=True, key=uuid.uuid4())
            else:
                st.plotly_chart(create_trade_chart(df, base_currency), use_container_width=True)
            
            # Display the raw data
            with st.expander("View Raw Data"):
                st.dataframe(df)
    
    async def refresh(self):
        while True:
            self.render()
            await asyncio.sleep(5)

    async def start_ws_task(self): 
        while True: 
            task = asyncio.create_task(self.fetch_from_websocket(), name='fetch_trade_data')
            await self.symbol_changed.wait()

            self.symbol_changed.clear()
            task.cancel()
            try: 
                await task
            except asyncio.CancelledError:
                logger.info("Task cancelled, starting new task...")
                continue

    async def start(self): 
        self.first_render()

        logger.info("Starting WebSocket consumer and database writers...")
        tasks = [
            asyncio.create_task(self.start_ws_task(), name='fetch_trade_data'),
            asyncio.create_task(self.refresh())
        ]
        self.tasks = tasks

        await asyncio.gather(*tasks)

    async def astop(self): 
        tasks = self.tasks
        for task in tasks: 
            logger.info(f"Cancelling task: {task.get_name()}")
            task.cancel()
            await task
        logger.info("All tasks cancelled.")

    def stop(self): 
        self.event_loop.run_until_complete(self.astop())

# Main app
async def main():
    trades = {}
    order_book = {}

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
        # df, base_currency = fetch_trade_data(engine, trading_pair, num_trades)
        df = pd.DataFrame()
        base_currency = 'btc_usdt'
        # res = fetch_order_book(engine, trading_pair)
        # print(f"Received order book data: {res}")

    df = pd.DataFrame(trades)
    
    if df.empty:
        st.warning("No data available. Please check your database connection and table name.")

    else: 
        print(f"Received trade data: {df}")
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

# if __name__ == "__main__":
#     loop = asyncio.new_event_loop()

print(f'session state: {st.session_state}')
loop = asyncio.new_event_loop()
# if 'dashboard' in st.session_state:
#     # st.session_state.dashboard.event_loop.run_until_complete(st.session_state.dashboard.stop())
#     st.session_state.dashboard.stop()
# if "dashboard" not in st.session_state:

st.session_state.dashboard = Dashboard()
# loop.create_task(st.session_state.dashboard.start())
loop.run_until_complete(st.session_state.dashboard.start())
