import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import sys
from streamlit.delta_generator import DeltaGenerator
import asyncio
import msgspec
import websockets
from loguru import logger
import uuid
import ciso8601
from crypto_hft.streamlit.util_functions import fetch_available_symbols, fetch_data_from_db, get_db_engine
import datetime
from io import BytesIO
from crypto_hft.streamlit.plotting_utils import (
    plot_contributions, 
    plot_features, 
    plot_xs_exchange_arb, 
    plot_order_book, 
    plot_ohlc, 
)
from crypto_hft.models import CrossExchangeArb, FairValueModel

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

# Page title
st.title("Crypto Dashboard")
# Sidebar controls
st.sidebar.header("Menu")
st.subheader("Trade Statistics")

trading_pair: DeltaGenerator = st.sidebar.selectbox(
    "Select Trading Pair",
    available_symbols,
    index=0,
    format_func=lambda s: s.upper().replace('_', '/'),
)

if st.session_state.get('symbol_data') is None:
    st.session_state['symbol_data'] = {}

logger.info(f'Symbols available in the session state: {list(st.session_state["symbol_data"].keys())}')
if st.session_state['symbol_data'].get(trading_pair) is None:
    st.session_state['symbol_data'][trading_pair] = {
        'trades': [],
        'order_book': []
    }

class Dashboard(): 
    def __init__(self) -> None: 
        """Initialize the dashboard with the selected trading pair and set up the WebSocket connection.
        """
        logger.warning("Initializing dashboard...")
        symbol_data = st.session_state['symbol_data'][trading_pair]

        self.trades = symbol_data['trades']
        self.order_book = symbol_data['order_book']
        
        self.xs_exchange_arb = CrossExchangeArb(
            symbols=[str(trading_pair)],
            liquid_exchange='binance',
            illiquid_exchanges=['kraken', 'poloniex', 'bybit', 'hyperliquid']
        )
        self.fair_value_model = FairValueModel(str(trading_pair))

        self.price_chart = None
        self.data_container: DeltaGenerator | None = None
        self.xs_exchange_arb_container: DeltaGenerator | None = None
        self.fair_value_model_container: DeltaGenerator | None = None

        self.engine = get_db_engine()

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)

    async def fetch_from_websocket(self) -> None: 
        """Task to continuously receive data from the WebSocket server and update the trades and order book.

        Parameters
        ----------
        trading_pair : str
            The trading pair to subscribe to.

        Raises
        ------
        ValueError
            If the message format is invalid or if the exchange is not supported.
        """
        logger.warning('initializing websocket consumer...')
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

                        data['timestamp'] = ciso8601.parse_datetime(data['timestamp'])
                        data['local_timestamp'] = ciso8601.parse_datetime(data['local_timestamp'])

                        if 'trade_id' in data:
                            # logger.debug(f"Received trade for exchange {data['exchange']}: {data}")
                            self.trades.append(data)
                            self.fair_value_model.update_trades(trade_data=data)
                        elif 'bid_0_px' in data: 
                            # logger.debug(f"Received order book update for excange {data['exchange']}: {data}")
                            self.order_book.append(data)
                            self.xs_exchange_arb.process_ob_update(
                                symbol=str(trading_pair),
                                exchange=data['exchange'],
                                price=data['bid_0_px'],
                                amount=data['bid_0_sz'],
                                side_is_bid=True
                            )

                            self.xs_exchange_arb.process_ob_update(
                                symbol=str(trading_pair),
                                exchange=data['exchange'],
                                price=data['ask_0_px'],
                                amount=data['ask_0_sz'],
                                side_is_bid=False
                            )

                            self.fair_value_model.update_order_book(order_book_data=data)
                        else:
                            raise ValueError("Invalid message format: 'trade_id' or bid not found in data")
                        
                except websockets.ConnectionClosed:
                    logger.error("Connection closed, attempting to reconnect...")

    def first_render(self) -> None: 
        """Render the initial layout of the dashboard, including the data downloader and the main content area.

        Creates the placeholders for the data container, cross-exchange arbitrage, and fair value model sections.
        """
        self.data_container = st.empty()

        st.header('Cross-Exchange Arbitrage')
        self.xs_exchange_arb_container = st.empty()

        st.header('Fair Value Model')
        self.fair_value_model_container = st.empty()

        self.download_box()

    def download_box(self): 
        """Display a data downloader box that allows the user to select a date range and download data as a Parquet file.

        The user can select the data type (orderbook, trades, funding, or currency_metadata) and specify a date range.
        """
        max_days = 7
        with st.container(): 
            st.header("Data Downloader")

            # Option picker
            data_type = st.selectbox("Select data type", ["orderbook", "trades", "funding", "currency_metadata"])

            # Date range picker
            today = datetime.datetime.now().date()
            dates = st.date_input(
                "Select date range",
                [today - datetime.timedelta(days=1), today],
                min_value=today - datetime.timedelta(days=365),
                max_value=today
            )

            # Validate range
            if isinstance(dates, tuple) and len(dates) > 1:  # Streamlit sometimes returns list
                start_date, end_date = dates[0], dates[1] # type: ignore
            elif isinstance(dates, tuple) and len(dates) == 1:
                start_date = dates[0]
                end_date = start_date + datetime.timedelta(days=1)
            elif isinstance(dates, datetime.date):
                start_date = dates
                end_date = start_date + datetime.timedelta(days=1)
            elif dates is None: 
                return

            if (end_date - start_date).days > max_days:
                st.error(f"Please select a date range of no more than {max_days} days.")
                return

            # Download button
            if st.button("Download data as Parquet"):
                logger.info(f"Downloading {data_type} data from {start_date} to {end_date}...")
                with st.spinner("Loading data from database..."):
                    df = fetch_data_from_db(self.engine, data_type, start_date, end_date)

                    buffer = BytesIO()
                    df.to_parquet(buffer, index=False)
                    buffer.seek(0)

                st.success("Data is ready!")

                st.download_button(
                    label="Click to download Parquet",
                    data=buffer,
                    file_name=f"{data_type}_{start_date}_to_{end_date}.parquet",
                    mime="application/octet-stream"
                )

    def render(self): 
        """Function called when data is updated and the dashboard needs to be re-rendered.
        """
        if len(self.trades) == 0:
            with self.data_container.container():
                st.warning("Warming up...")
            
            return

        # print(f'selected trading pair: {trading_pair}')
        df = pd.DataFrame(self.trades)  
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['local_timestamp'] = pd.to_datetime(df['timestamp'])
        timeframe = '1min'
        base_currency = trading_pair.split('_')[0].upper()

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

            chart_cols = st.columns([.7, .3], vertical_alignment='center')
            with chart_cols[0]:
                st.plotly_chart(plot_ohlc(df, base_currency, timeframe), use_container_width=True, key=uuid.uuid4())

            with chart_cols[1]:
                if len(self.order_book) > 0:
                    st.dataframe(plot_order_book(self.order_book[-1]), use_container_width=True, hide_index=True)
            
            # Display the raw data
            with st.expander("View Raw Trades Data"):
                st.dataframe(df)

        # Cross-Exchange Arbitrage
        with self.xs_exchange_arb_container.container():
            arbs = self.xs_exchange_arb.compute_arbs_for_symbol(trading_pair)
            plot = plot_xs_exchange_arb(arbs)
            st.plotly_chart(plot, use_container_width=True, key=uuid.uuid4())

        with self.fair_value_model_container.container():
            result = self.fair_value_model.run()
            if result is None: 
                return
            
            data, contributions = result
            
            cols = st.columns(2)
            with cols[0]: 
                st.plotly_chart(plot_features(data), use_container_width=True, key=uuid.uuid4())
            with cols[1]:
                st.plotly_chart(plot_contributions(contributions), use_container_width=True, key=uuid.uuid4())
        
    async def refresh(self):
        """Time the refresh rate of the dashboard.

        This function is called every 2 seconds to update the dashboard with new data.
        It buffers to 2 seconds to avoid reloading every time data is received (which happens at sub-second intervals).
        """
        while True:
            self.render()
            await asyncio.sleep(2)

    async def start(self): 
        """Start the dashboard by initializing the WebSocket connection and starting the refresh task.
        """
        self.first_render()

        logger.info("Starting WebSocket consumer and database writers...")
        tasks = [
            asyncio.create_task(self.fetch_from_websocket(), name='fetch_trade_data'),
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

loop = asyncio.new_event_loop()

st.session_state.dashboard = Dashboard()
# loop.create_task(st.session_state.dashboard.start())
loop.run_until_complete(st.session_state.dashboard.start())
