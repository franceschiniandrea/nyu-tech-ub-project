from crypto_hft.utils.config import Config

config = Config()

EXCHANGE_SYMBOLS = {}
REVERSE_SYMBOL_MAP: dict[str, dict] = {exchange: {} for exchange in config.exchanges}

# ✅ Define exchange-specific symbol formats for WebSocket input
EXCHANGE_MAPPING_RULES = {
    "hyperliquid": lambda base, quote: base.lower(),  # ✅ Hyperliquid only takes base (e.g., "btc")
    "binanceus": lambda base, quote: f"{base}{quote}".lower(),  # ✅ "btcusdt"
    "binance": lambda base, quote: f"{base}{quote}".lower(),  # ✅ "btcusdt"
    "poloniex": lambda base, quote: f"{base}_{quote}",  # ✅ "BTC_USDT"
    "coinbase": lambda base, quote: f"{base.upper()}-{quote.replace('usdt', 'USD')}",  # ✅ "BTC-USD"
}

# ✅ Define exchange-specific output mapping for WebSocket response → Database format
EXCHANGE_OUTPUT_MAPPING_RULES = {
    "hyperliquid": lambda symbol: f"{symbol.upper()}_USDT",  # BTC → BTC_USDT
    "binanceus": lambda symbol: f"{symbol[:-4].upper()}_USDT" if symbol.endswith("usdt") else symbol,  # ✅ BTCUSD → BTC_USDT, ETHUSD → ETH_USDT
    "binance": lambda symbol: f"{symbol[:-4].upper()}_USDT" if symbol.endswith("usdt") else symbol,  # ✅ BTCUSD → BTC_USDT, ETHUSD → ETH_USDT
    "poloniex": lambda symbol: symbol,  # No change (already BTC_USDT)
    "coinbase": lambda symbol: f"{symbol.split('-')[0]}_USDT"  # ✅ BTC-USD → BTC_USDT
}

def map_symbols(exchange, tickers):
    """
    Maps tickers to the format required by each exchange for WebSocket requests.
    Returns:
        list: Mapped tickers in the correct format for WebSocket requests.
        dict: Reverse mapping for database insertion.
    """
    mapped_list = []  # WebSocket needs a list
    mapped_dict = {}  # Database needs a dictionary

    for ticker in tickers:
        base, quote = ticker.split("_")
        base, quote = base.lower(), quote.lower()  # Precompute lowercase once

        mapped = EXCHANGE_MAPPING_RULES.get(exchange, lambda b, q: f"{b}_{q}")(base, quote)

        mapped_list.append(mapped)
        mapped_dict[mapped] = ticker  # Reverse lookup for database

    return mapped_list, mapped_dict

# Generate symbol mappings
for exchange in config.exchanges:
    EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, config.base_tickers)

def reverse_map_symbol(exchange, symbol):
    """
    Converts the exchange-specific output symbol format back to the standard 'BTC_USDT' format.
    """
    return EXCHANGE_OUTPUT_MAPPING_RULES.get(exchange, lambda s: s)(symbol)

# print("Coinbase Subscribed Symbols:", EXCHANGE_SYMBOLS["coinbase"])
