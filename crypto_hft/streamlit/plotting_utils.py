import polars as pl
import numpy as np
import pandas as pd
import plotly.graph_objects as go # type: ignore
import plotly.express as px # type: ignore

# Function to create a candlestick chart
def plot_ohlc(df: pd.DataFrame, base_currency: str, timeframe='1min'):
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
def plot_trades(df, base_currency):
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

def plot_xs_exchange_arb(df: pl.LazyFrame): 
    data = (
        df
        .filter(pl.col('buy_illiquid_bps').is_not_nan() | pl.col('sell_illiquid_bps').is_not_nan())
        .collect(engine='streaming')
    )
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=data['illiquid_exchange'],
        y=data['buy_illiquid_bps'],
        name='Buy Illiquid',
    ))
    fig.add_trace(go.Bar(
        x=data['illiquid_exchange'],
        y=data['sell_illiquid_bps'],
        name='Sell Illiquid',
    ))

    # Update layout
    fig.update_layout(
        title='XS-Exchange Arb',
        xaxis_title='Venue',
        yaxis_title='Arbitrage (bps)',
        height=600
    )

    return fig

def plot_features(features: pd.DataFrame) -> go.Figure: 
    feature_names = ['ob_imbalance', 'flow_imbalance_ewm', 'breakout']
    data = features.loc[:, feature_names].melt(ignore_index=False)
    fig = px.line(
        data,
        x=data.index,
        y='value',
        color='variable',
        title='Features'
    )
    return fig

def plot_contributions(contributions: pd.DataFrame) -> go.Figure:
    data = (
        contributions
        .melt(ignore_index=False)
        .rename(columns={'variable': 'feature', 'value': 'contribution'}))
    fig = px.bar(
        data,
        x=data.index,
        y='contribution',
        color='feature',
        title='Feature Contributions'
    )
    return fig

def plot_order_book(snapshot: dict): 
    # Extract bids and asks
    bids = [{'Price': snapshot[f'bid_{i}_px'], 'Size': snapshot[f'bid_{i}_sz'], 'Side': 'Bid'} for i in range(5)]
    asks = [{'Price': snapshot[f'ask_{i}_px'], 'Size': snapshot[f'ask_{i}_sz'], 'Side': 'Ask'} for i in range(5)]

    bids_df = pd.DataFrame(bids)
    asks_df = pd.DataFrame(asks).sort_values('Price', ascending=True)
    mid_df = pd.DataFrame({'Price': [(snapshot['bid_0_px'] + snapshot['ask_0_px']) / 2], 'Size': [np.nan], 'Side': 'Mid'})

    book = pd.concat([
        asks_df,
        mid_df,
        bids_df,
    ]).reset_index(drop=True)

    def highlight_side(row):
        if row['Side'] == 'Bid':
            return ['background-color: rgba(0, 255, 0, 0.1)'] * len(row)
        elif row['Side'] == 'Ask':
            return ['background-color: rgba(255, 0, 0, 0.1)'] * len(row)
        return [''] * len(row)

    return book.style.apply(highlight_side, axis=1).format({"Price": "{:,.4f}", "Size": "{:,.2f}"})
