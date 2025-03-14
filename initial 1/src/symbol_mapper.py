from config import EXCHANGES, BASE_TICKERS

# Dictionary for exchange-specific symbol mappings (for WebSocket requests)
EXCHANGE_SYMBOLS = {}

# Dictionary for reverse lookup per exchange (for database insertion)
REVERSE_SYMBOL_MAP = {exchange: {} for exchange in EXCHANGES}

def map_symbols(exchange, tickers):
    """
    Maps tickers to the format required by each exchange.
    Returns:
        list: Mapped tickers in the correct format for WebSocket requests.
        dict: Reverse mapping for database insertion.
    """
    mapped_list = []  # WebSocket needs a list
    mapped_dict = {}  # Database needs a dictionary

    for ticker in tickers:
        base, quote = ticker.split("_")

        if exchange == "hyperliquid":
            mapped = base.lower()  # Hyperliquid uses lowercase base (e.g., "btc")

        elif exchange == "binance-us":
            mapped = f"{base.lower()}{quote.lower()}"  # Binance US uses merged format (e.g., "btcusdt")

        elif exchange == "poloniex":
            mapped = f"{base}_{quote}"  # Poloniex uses "BASE_QUOTE" format (e.g., "BTC_USDT")

        elif exchange == "coinbase":
            mapped = f"{base}-{quote.replace('USDT', 'USD')}"  # Coinbase uses "BASE-USD" (e.g., "BTC-USD")

        else:
            mapped = ticker  # Default: return as-is

        mapped_list.append(mapped)  # Store for WebSocket
        mapped_dict[mapped] = ticker  # Store reverse lookup for database

    return mapped_list, mapped_dict

# Generate symbol mappings for each exchange
for exchange in EXCHANGES:
    EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, BASE_TICKERS)

# Testing Output
if __name__ == "__main__":
    for exchange, symbols in EXCHANGE_SYMBOLS.items():
        print(f"{exchange}: {symbols}")
    print("\nReverse Symbol Map:")
    for exchange, reverse_map in REVERSE_SYMBOL_MAP.items():
        print(f"{exchange}: {reverse_map}")