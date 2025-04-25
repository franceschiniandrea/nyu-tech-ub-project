import polars as pl
import numpy as np
from loguru import logger
from itertools import product

class LastQuote():
    """Class storing the last quote for a given market (exchange + symbol).

    The `best_bid` and `best_ask` are tuples of (price, amount) for the best bid and ask prices respectively.
    The midprice is the average of the best bid and best ask prices.
    """
    midprice: float
    best_bid: tuple[float, float]
    best_ask: tuple[float, float]

    def __init__(self, midprice: float, best_bid: tuple[float, float], best_ask: tuple[float, float]):
        self.midprice = midprice
        self.best_bid = best_bid
        self.best_ask = best_ask

    def update(self, price: float, amount: float, side_is_bid: bool) -> None: 
        """Update the last quote with a new price and amount for a given side.

        Parameters
        ----------
        price : float
            The new price at the best level.
        amount : float
            The new amount at the best level.
        side_is_bid : bool
            Whether the update is for the bid or ask side.
        """
        if side_is_bid: 
            self.best_bid = (price, amount)
        else:
            self.best_ask = (price, amount)
        
        # recompute the midprice
        self.midprice = (self.best_bid[0] + self.best_ask[0]) / 2

    @classmethod
    def empty_quote(cls) -> 'LastQuote':
        """Create an empty LastQuote object with NaN values."""
        return cls(
            midprice=np.nan,
            best_bid=(np.nan, np.nan),
            best_ask=(np.nan, np.nan)
        )

class CrossExchangeArb(): 
    def __init__(self, symbols: list[str], liquid_exchange: str, illiquid_exchanges: list[str]):
        self.symbols = symbols
        self.liquid_exchange = liquid_exchange
        self.illiquid_exchanges = illiquid_exchanges

        self.price_data: dict[tuple[str, str], LastQuote] = {
            (symbol, exchange): LastQuote.empty_quote() for symbol, exchange in product(symbols, illiquid_exchanges + [liquid_exchange])
        }
        """Dictionary mapping `(symbol, exchange)` to `LastQuote`.
        """

    def process_ob_update(self, symbol: str, exchange: str, price: float, amount: float, side_is_bid: bool): 
        """Process an update for a given symbol and exchange.

        This will be the function you call when receiving an order book update
        from the websocket. For this first implementation we just care about 
        the bbo and not the full book.
        The function will update the last quote for the given symbol and exchange.

        Parameters
        ----------
        symbol : str
            The symbol to process the update for.
        exchange : str
            The exchange the update is coming from.
        price : float
            The new price at the best level.
        amount : float
            The new amount at the best level.
        side_is_bid : bool
            Whether the update is for the bid or ask side.
        """
        last_quote_symbol = self.price_data.get((symbol, exchange), None)

        if last_quote_symbol is None:
            last_quote_symbol = LastQuote.empty_quote()
        
        # update the last stored quote
        last_quote_symbol.update(price=price, amount=amount, side_is_bid=side_is_bid)

    def compute_arbs_for_symbol(self, symbol: str) -> pl.LazyFrame: 
        """Compute the arbitrage in bps when buying on the illiquid exchange and selling on the liquid exchange. 

        Assumes making on the illiquid exchange and taking on the liquid exchange,
        meaning you buy at the bid on the illiquid exchange and sell at the bid on the liquid exchange
        in the case of a the vice versa for sell_illiquid.

        Parameters
        ----------
        symbol : str
            The symbol to compute the arbitrage for.

        Returns
        -------
        pl.LazyFrame
            A polars LazyFrame containing, for each illiquid exchange, the exchange name, 
            the buy_illiquid_bps and sell_illiquid_bps arbitrages.
        """
        illiquid_exchanges = self.illiquid_exchanges
        liquid_quote = self.price_data[(symbol, self.liquid_exchange)]
        
        # get the quotes for the liquid exchange
        liquid_ask = liquid_quote.best_ask
        liquid_bid = liquid_quote.best_bid

        # initialize the result array which will be later converted to a df
        res = np.empty((len(illiquid_exchanges), 3), dtype='<U32')
        
        for i in range(len(illiquid_exchanges)):
            # get the bbo from the illiquid exchange
            illiquid_exchange = illiquid_exchanges[i] 
            illiquid_quote = self.price_data[(symbol, illiquid_exchange)]
            illiquid_bid = illiquid_quote.best_bid
            illiquid_ask = illiquid_quote.best_ask

            # if either of the sides is NaN return NaN
            if np.isnan(liquid_bid[0]) or np.isnan(illiquid_bid[0]):
                logger.warning(f'Returning NaN for buy illiquid for symbol {symbol} on {illiquid_exchange}, since one of the legs is NaN')
                buy_illiquid_edge = np.nan
            else: 
                # buy at bid on illiquid, sell at bid on liquid
                buy_illiquid_edge = (liquid_bid[0] / illiquid_bid[0] - 1) * 1e4
            
            # if either of the sides is NaN return NaN
            if np.isnan(liquid_ask[0]) or np.isnan(illiquid_ask[0]):
                logger.warning(f'Returning NaN for sell illiquid for symbol {symbol} on {illiquid_exchange}, since one of the legs is NaN')
                sell_illiquid_edge = np.nan
            else: 
                # buy at ask on illiquid, sell at ask on liquid
                sell_illiquid_edge = (illiquid_ask[0] / liquid_ask[0] - 1) * 1e4 

            res[i, :] = (illiquid_exchange, buy_illiquid_edge, sell_illiquid_edge)

        cols = {'illiquid_exchange': pl.String(), 'buy_illiquid_bps': pl.Float64(), 'sell_illiquid_bps': pl.Float64()}
        df = pl.LazyFrame(res, schema=cols).with_columns(symbol=pl.lit(symbol))

        return df
    
    def compute_all_arbs(self) -> pl.LazyFrame: 
        """Compute the arbitrage for all symbols.

        Returns
        -------
        pl.LazyFrame
            A lazy frame containing the arbitrage for all symbols.
        """
        dfs: list[pl.LazyFrame] = []
        for symbol in self.symbols: 
            df = self.compute_arbs_for_symbol(symbol)
            dfs.append(df)

        return pl.concat(dfs)

# test the function by running the code below
if __name__ == '__main__': 
    symbols = ['btc_usdt', 'eth_usdt']
    liquid_exchange = 'binance'
    illiquid_exchanges = ['kraken', 'poloniex']

    arb = CrossExchangeArb(symbols, liquid_exchange, illiquid_exchanges)

    # initialize all sides for btc
    arb.process_ob_update('btc_usdt', 'binance', 10000, 1, True) # binance bid
    arb.process_ob_update('btc_usdt', 'binance', 10020, 1, False) # binance ask
    arb.process_ob_update('btc_usdt', 'kraken', 9998, 1, True) # kraken bid
    arb.process_ob_update('btc_usdt', 'kraken', 10002, 1, False) # kraken ask

    # only some for eth_usdt
    arb.process_ob_update('eth_usdt', 'binance', 2000, 1, True) # binance bbo   
    arb.process_ob_update('eth_usdt', 'binance', 2050, 1, False) # binance bbo
    arb.process_ob_update('eth_usdt', 'kraken', 1999, 1, True) # only bid on kraken
    arb.process_ob_update('eth_usdt', 'poloniex', 1990, 1, True) # bid and ask on poloniex
    arb.process_ob_update('eth_usdt', 'poloniex', 2010, 1, False)

    logger.debug(f'Computed arbs for btc: {arb.compute_arbs_for_symbol("btc_usdt").collect(engine='streaming')}')
    logger.debug(f'All arbs: {arb.compute_all_arbs().collect(engine='streaming')}')