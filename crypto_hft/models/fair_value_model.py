import polars as pl
import numpy as np
from typing import Literal
import polars.selectors as cs
from itertools import product
from loguru import logger
from pathlib import Path
from statsmodels.iolib.smpickle import load_pickle # type: ignore
from statsmodels.regression.linear_model import OLSResults # type: ignore
import statsmodels.api as sm # type: ignore
import pandas as pd

_bbo_cols_px: dict[str, pl.DataType] = {
    f'{side}_{i}_px': pl.Float64()
    for side, i in product(['bid', 'ask'], range(10))
}

_bbo_cols_sz: dict[str, pl.DataType] = {
    f'{side}_{i}_sz': pl.Float64()
    for side, i in product(['bid', 'ask'], range(10))
}

_order_book_schema = pl.Schema({
    'timestamp': pl.Datetime(),
    'symbol': pl.String(),
    'exchange': pl.String(),
} | _bbo_cols_px | _bbo_cols_sz)

_trades_schema = pl.Schema({
    'timestamp': pl.Datetime(),
    'symbol': pl.String(),
    'exchange': pl.String(),
    'trade_id': pl.Int64(),
    'price': pl.Float64(),
    'amount': pl.Float64(),
    'side': pl.String(),
})

def _compute_imbalance_for_side(df: pl.LazyFrame, side: Literal['bid', 'ask']) -> pl.LazyFrame:
    col_base_name = side
    price_cols = [f'{col_base_name}_{i}_px' for i in range(10)]
    amount_cols = [f'{col_base_name}_{i}_sz' for i in range(10)]
    sign_correction = -1 if side == 'bid' else 1
    
    df = (
        df
        .with_columns(
            ((pl.concat_list(price_cols) / pl.col('midprice') - 1) * sign_correction * 1e4).alias(f'{col_base_name}_distances'),
            pl.concat_list(amount_cols).alias(f'{col_base_name}_amounts')
        )
        .with_columns(
            (1/pl.col(f'{col_base_name}_distances').list.eval(pl.element().pow(1/3))).alias(f'{col_base_name}_weights_raw')
        )
        .with_columns(
            (pl.col(f'{col_base_name}_weights_raw') / pl.col(f'{col_base_name}_weights_raw').list.drop_nulls().list.sum()).alias(f'{col_base_name}_weights_normalized')
        )
        .with_columns(
            (pl.col(f'{col_base_name}_weights_normalized') * pl.col(f'{col_base_name}_amounts')).list.sum().alias(f'{col_base_name}_imbalance')
        )
    )

    # logger.debug(f"Computed {side} imbalance with columns: {df.columns}:\n{df.head(5).collect(engine='streaming')}")
    return df

def _ohlcv(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df
        .sort('timestamp')
        .group_by_dynamic('timestamp', every='5s', closed='right', label='right')
        .agg(
            pl.col('price').last().alias('close'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            (pl.col('amount') * pl.col('price')).sum().alias('volume'),
            (pl.col('amount') * pl.col('price') * pl.col('side').eq('buy')).sum().alias('buy_volume'),
            (pl.col('amount') * pl.col('price') * pl.col('side').eq('sell')).sum().alias('sell_volume'),
        )
    )

def _compute_breakout_signal(data: pl.LazyFrame, window_size: int, buffer_multiple: int) -> pl.LazyFrame:
    # this data should be OHLCV data from trades

    data = (
        data
        .sort('timestamp')
        .with_columns(
            (np.abs(pl.col("high")-pl.col("close").shift())).alias("max_minus_close"),
            (np.abs(pl.col("close").shift()-pl.col('low'))).alias("close_minus_low"),
            (pl.col("high")-pl.col("low")).alias("high_minus_low")
        )
        .with_columns(
            pl.max_horizontal("max_minus_close","close_minus_low","high_minus_low").alias("tr")
        )
        .with_columns(
            pl.col("tr").rolling_mean(window_size=window_size, min_samples=5).alias("atr")
        )
        .with_columns(
            pl.col('close').rolling_max(window_size=window_size, min_samples=5).alias('high_400'),
            pl.col('close').rolling_min(window_size=window_size, min_samples=5).alias('low_400'),
        )
        .with_columns(
            up_threshold=pl.col('high_400').shift(1) + buffer_multiple * pl.col('atr'),
            down_threshold=pl.col('low_400').shift(1) - buffer_multiple * pl.col('atr')
        )
        .with_columns(
            breakout=pl.when((pl.col("close")-pl.col("up_threshold")>0))
            .then((pl.col("close")-pl.col("up_threshold")))
            .when(pl.col("down_threshold")-pl.col("close")>0)
            .then(pl.col("close")-pl.col("down_threshold"))
            .otherwise(0.)
        )
        .drop(['high_400', 'low_400', 'up_threshold', 'down_threshold', 'tr', 'atr', 'max_minus_close', 'close_minus_low', 'high_minus_low'])
    )
    # logger.debug(f"Computed breakout signal with columns: {data.columns}:\n{data.drop('high', 'low', 'close', 'buy_volume', 'sell_volume').tail(5).collect(engine='streaming')}")
    return data

def _compute_ob_imbalance_signal(df: pl.LazyFrame) -> pl.LazyFrame: 
    return (
        df
        .with_columns(
            pl.all().exclude('timestamp').replace(np.nan, None)
        )
        .pipe(_compute_imbalance_for_side, side='bid')
        .pipe(_compute_imbalance_for_side, side='ask')
        .with_columns(
            ob_imbalance=(pl.col('bid_imbalance') - pl.col('ask_imbalance'))
        )
        .drop(cs.ends_with('_distances', '_normalized', '_amounts', '_raw'))
    )

def compute_flow_imbalance_signal(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df
        .with_columns(
            flow_imbalance=(pl.col('buy_volume') - pl.col('sell_volume')) / pl.col('volume')
        )
        .with_columns(
            flow_imbalance_ewm=pl.col('flow_imbalance').ewm_mean(span=12)
        )
    )

class FairValueModel(): 
    def __init__(self, symbol) -> None: 
        self.symbol = symbol
        self.order_book = pl.LazyFrame(schema=_order_book_schema)
        self.trades = pl.LazyFrame(schema=_trades_schema)

        self.model: OLSResults = self.load_model()

    def load_model(self): 
        model_path = Path(__file__).parent.joinpath('model.pkl').resolve()
        return load_pickle(model_path)

    def update_trades(self, trade_data): 
        if trade_data['exchange'] != 'binance':
            # logger.debug(f"Exchange {trade_data['exchange']} not supported, skipping update.")
            return
        
        trade_df = pl.LazyFrame([trade_data], schema=_trades_schema)
        self.trades = pl.concat([self.trades, trade_df], how='vertical')

    def update_order_book(self, order_book_data: dict): 
        if order_book_data['exchange'] != 'binance':
            # logger.debug(f"Exchange {order_book_data['exchange']} not supported, skipping update.")
            return
        
        ob_update = pl.LazyFrame([order_book_data], schema=_order_book_schema)
        self.order_book = pl.concat([self.order_book, ob_update], how='vertical')

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame] | None:
        signals = self._compute_signals()

        if signals is None: 
            return None
        
        signals_processed = (
            signals
            .select('timestamp', 'midprice', 'breakout', 'ob_imbalance', 'flow_imbalance_ewm')
            .with_columns(
                log_return=pl.col('midprice').log().diff() * 1e4,
            )
            .collect(engine='streaming')
            .to_pandas()
            .set_index('timestamp')
        ) # type: ignore

        # normalize the signals
        xcols = ['ob_imbalance', 'flow_imbalance_ewm']

        # normalize the breakout signal
        mask = signals_processed['breakout'] != 0.
        signals_processed.loc[mask, 'breakout'] /= signals_processed.loc[mask, 'breakout'].ewm(halflife=12).std()

        # normalize the rest of the signals
        signals_processed[xcols] /= signals_processed[xcols].ewm(halflife=12).std()
        X = signals_processed[['breakout'] + xcols]
        X = sm.add_constant(X)

        # the predicted value is the vol-adjusted log return of midprice
        signals_processed['log_return_30s_predicted'] = self.model.predict(X)

        contributions = X * self.model.params
        # logger.info(f'contributions:\n{contributions.tail(5)}')
        return signals_processed, contributions

    def _compute_signals(self) -> pl.LazyFrame | None: 
        if self.order_book.select(pl.count()).collect(engine='streaming').item() == 0:
            # logger.debug("No order book data available, skipping signal computation.")
            return None
        ob_signals = (
            self.order_book
            .with_columns(
                midprice=(pl.col('bid_0_px') + pl.col('ask_0_px')) / 2,
            )
            .pipe(_compute_ob_imbalance_signal)
            # .pipe(self._compute_flow_imbalance_signal)
            # .pipe(self._compute_breakout_signal)
        )

        trades_signal = (
            self.trades
            .pipe(_ohlcv)
            .pipe(_compute_breakout_signal, window_size=12, buffer_multiple=3)
            .pipe(compute_flow_imbalance_signal)
        )

        # logger.debug(f'trades signal:\n{trades_signal.tail(5).collect(engine="streaming")}')
        # logger.debug(f'order book signal:\n{ob_signals.tail(5).collect(engine="streaming")}')

        signals = ob_signals.join_asof(
            trades_signal,
            on='timestamp',
            strategy='backward',
        )
        # logger.debug(f'joined signals:\n{signals.tail(5).collect(engine="streaming")}')

        return signals

if __name__ == '__main__':
    # Example usage
    model = FairValueModel("BTCUSDT")
    model.run()

# data should contain (i) bid asks, (ii) mids