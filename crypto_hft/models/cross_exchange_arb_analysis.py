# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import polars as pl
from dotenv import load_dotenv
import os
import psycopg2
from loguru import logger
import polars.selectors as cs
from pathlib import Path

load_dotenv()


# %% [markdown]
# ## Getting the data from the DB

# %%
class DBConnector(): 
    POSTGRES_HOST: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DATABASE: str
    POSTGRES_PORT: str

    def __init__(self): 
        self.load_env_vars()

        self.db_config = {
            'user': self.POSTGRES_USER,
            'password': self.POSTGRES_PASSWORD,
            'host': self.POSTGRES_HOST,
            # 'port': self.POSTGRES_PORT,
            'database': self.POSTGRES_DATABASE
        }

    def load_env_vars(self): 
        load_dotenv()

        required_vars = [
            'POSTGRES_HOST',
            'POSTGRES_USER',
            'POSTGRES_PASSWORD',
            'POSTGRES_DATABASE',
            'POSTGRES_PORT'
        ]
        for var in required_vars: 
            if os.getenv(var) is None: 
                raise ValueError(f"Environment variable {var} not set")
            setattr(self, var, os.getenv(var))
    
    def list_tables(self): 
        db_config = self.db_config
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE';
                """)
                return pd.DataFrame(cur.fetchall())
            
    def load_data(self, data_type: str, symbol: str, query: str): 
        """Load data from the database.

        The query needs to contain a format string for `table_name` which will be replaced at runtime.

        Example query: 
            SELECT * FROM {table_name} 
            WHERE 
                timestamp >= '2024-04-20T00:00:00+00:00'::timestamptz
        """
        assert data_type in ['trades', 'orderbook'], "data_type must be either 'trades' or 'orderbook'"
        # assert symbol.contains('_'), "symbol must contain '_'"
        symbol = symbol.lower()

        connection = psycopg2.connect(**self.db_config)
        table_name = f"{data_type}_{symbol}"
        
        logger.info(f"Loading {data_type} data for {symbol} from table {table_name}")
        df = pl.read_database(query.format(table_name=table_name), connection=connection)

        return df.with_columns(
            pl.lit(symbol).alias('symbol')
        )


# %%
db = DBConnector()

tables = db.list_tables()
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(tables)

# %%
TICKER = 'op_usdt'
data = db.load_data("orderbook", TICKER, """
    SELECT * FROM {table_name}
    WHERE 
        timestamp >= '2025-04-24'::timestamptz
        AND timestamp < '2025-04-25'::timestamptz
""")

data.head()

# %%
data.write_parquet(f'{TICKER}_2025-04-24.pq', compression='zstd')

# %% [markdown]
# # Analysis

# %% [markdown]
# ## Importing Data

# %%
base_dir = Path('../../data')

files = [
    str(base_dir.joinpath(f'{ticker}_usdt_2025-04-24.pq').resolve())
    for ticker in ['dash', 'op', 'pepe', 'link']
]

dfs = []
for file in files: 
    data = pl.scan_parquet(file).with_columns(
        cs.starts_with('bid_').cast(pl.Float64()),
        cs.starts_with('ask_').cast(pl.Float64()),
    ).with_columns(
        midprice=(pl.col('bid_0_px') + pl.col('ask_0_px')) / 2,
    )
    dfs.append(data)
data = pl.concat(dfs)

data.head().collect(engine='streaming')

# %%
data.select(pl.len()).collect(engine='streaming').item(), data.unique().select(pl.len()).collect(engine='streaming').item()

# %%
data.select(pl.col('exchange').unique()).collect(engine='streaming')    

# %% [markdown]
# ## Actual Analysis

# %% [markdown]
# ## Other stuff

# %%
df = data.collect(engine='streaming').to_pandas()

# %%
prices = df.copy()

prices.set_index(['timestamp', 'symbol'], inplace=True)
prices.sort_index(inplace=True)

# %%
prices.head()

# %%
test_symbol = prices.xs('dash_usdt', axis=0, level=1)
test_symbol = test_symbol[test_symbol['exchange'] == 'poloniex']

test_symbol.sort_index(inplace=True)
test_symbol['bid_0_px'].plot(label='bid')
test_symbol['ask_0_px'].plot(label='ask')

plt.legend()

# %%
test_symbol[['bid_0_px', 'ask_0_px']]

# %% [markdown]
# ## Average Arbitrage Opportunity

# %%
# try maker on the illiquid exchange, taker on the liquid exchange
#   so make bid on illiquid, take bid on liquid
#   or make ask on illiquid, take ask on liquid
#   the bps of profit is bid_liquid - bid_illiquid / bid_illiquid = bid_liquid / bid_illiquid - 1

# for make on the ask: ask_illiquid - ask_liquid / ask_liquid = ask_illiquid / ask_liquid - 1

# buy_illiquid = bid_liquid / bid_illiquid - 1
# sell_illiquid = ask_illiquid / ask_liquid - 1

# %%
uniusdt = prices.xs('dash_usdt', axis=0, level=1).sort_index()

uniusdt.head(5)

# %%
second_sampling = uniusdt.groupby('exchange')[['ask_0_px', 'bid_0_px', 'midprice', 'bid_0_sz', 'ask_0_sz']].resample('5s').last()
second_sampling.tail()

# %%
second_sampling = uniusdt.groupby('exchange').resample('5s').last().drop('exchange', axis=1)

buy_illiquid = (second_sampling.loc['binance', 'bid_0_px'] / second_sampling.loc['poloniex', 'bid_0_px']) \
    .sub(1) \
    .mul(1e4) \
    .dropna() \
    .rename('buy_illiquid')

sell_illiquid = (second_sampling.loc['poloniex', 'ask_0_px'] / second_sampling.loc['binance', 'ask_0_px']) \
    .sub(1) \
    .mul(1e4) \
    .dropna() \
    .rename('sell_illiquid')

buy_illiquid.clip(-100, 100).plot(title='buy_illiquid arbitrage, resampled every 5s', ylabel='bps')
sell_illiquid.clip(-100, 100).plot(title='sell_illiquid arbitrage, resampled every 5s', ylabel='bps')


# %%
bid_quoted_size_pol = second_sampling.loc['poloniex', 'bid_0_sz'].rename('bid_size_poloniex')
ask_quoted_size_pol = second_sampling.loc['poloniex', 'ask_0_sz'].rename('ask_size_poloniex')
bid_quoted_size_bin = second_sampling.loc['binance', 'bid_0_sz'].rename('bid_size_binance')
ask_quoted_size_bin = second_sampling.loc['binance', 'ask_0_sz'].rename('ask_size_binance')

overview_arb = pd.concat([
    buy_illiquid,
    sell_illiquid,
    bid_quoted_size_pol,
    ask_quoted_size_pol,
    bid_quoted_size_bin,
    ask_quoted_size_bin
], axis=1)

overview_arb.head()

# %%
overview_arb['profitable_trade'] = np.where(overview_arb.buy_illiquid > overview_arb.sell_illiquid, overview_arb.buy_illiquid, overview_arb.sell_illiquid)
ax = overview_arb['profitable_trade'].rolling(12).mean().clip(-100, 100).plot(title='buy_illiquid arbitrage, resampled every 5s', ylabel='bps')
ax.axhline(0, color='red', linestyle='--')

# %% [markdown]
# ## Now for all the cryptos

# %%
prices.head()

# %%
resampled_quotes = prices.reset_index(level=1).sort_index().groupby(['symbol', 'exchange']).resample('1s', include_groups=False).last()
resampled_quotes = resampled_quotes.swaplevel(0,1).swaplevel().sort_index()
resampled_quotes.head()

# %%
buy_illiquid = (resampled_quotes.loc['binance', 'bid_0_px'] / resampled_quotes.loc['poloniex', 'bid_0_px']) \
    .sub(1) \
    .mul(1e4) \
    .dropna() \
    .rename('buy_illiquid')

sell_illiquid = (resampled_quotes.loc['poloniex', 'ask_0_px'] / resampled_quotes.loc['binance', 'ask_0_px']) \
    .sub(1) \
    .mul(1e4) \
    .dropna() \
    .rename('sell_illiquid')

overview_arb = pd.concat([
    buy_illiquid,
    sell_illiquid,
], axis=1)

overview_arb.head()

# %%
overview_arb.index.get_level_values(1).unique()

# %%
symbols = overview_arb.index.get_level_values(1).unique()
nplots = len(symbols)
# nplots = 2 
fig, axs = plt.subplots(nplots, 1, figsize=(10, nplots * 4), sharex=True)

for i, symbol in enumerate(symbols):
    print(f'displaying {symbol}')
    ax = axs[i]
    overview_arb.swaplevel().loc[symbol].rolling(60).mean().clip(-100, 100).plot(ax=ax, title=symbol)

fig.suptitle('Arbitrage opportunities for all symbols, rolling 1min mean', fontsize=16)

# %%
overview_arb.groupby(level=1).mean()

# %%
edge = 111e-4 # edge in bps
size = 500
withdrawal_cost_coin = 0.06131070
coin_price = 13.96

edge * size - withdrawal_cost_coin * coin_price - 1

# %%
breakeven = (withdrawal_cost_coin * coin_price + 1) / edge
breakeven
