from config import EXCHANGES, BASE_TICKERS

# Dictionary for exchange-specific symbol mappings (for WebSocket requests)
EXCHANGE_SYMBOLS = {}
REVERSE_SYMBOL_MAP = {exchange: {} for exchange in EXCHANGES}

# Define exchange-specific symbol formats
EXCHANGE_MAPPING_RULES = {
    "hyperliquid": lambda base, quote: base.lower(),  # ✅ FIX: Hyperliquid only takes base (e.g., "btc")
    "binance-us": lambda base, quote: f"{base}{quote}".lower(),  # ✅ "btcusdt"
    "binance": lambda base, quote: f"{base}{quote}".lower(),  # ✅ "btcusdt"
    "poloniex": lambda base, quote: f"{base}_{quote}",  # ✅ "BTC_USDT"
    "coinbase": lambda base, quote: f"{base}-{quote.replace('USDT', 'USD')}",  # ✅ "BTC-USD"
}


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
        base, quote = base.lower(), quote.lower()  # Precompute lowercase once

        # Special handling for Hyperliquid (only uses base)
        if exchange == "hyperliquid":
            mapped = base.lower()  # Expects just "btc", "eth", etc.
        else:
            mapped = EXCHANGE_MAPPING_RULES.get(exchange, lambda b, q: f"{b}_{q}")(base, quote)

        mapped_list.append(mapped)
        mapped_dict[mapped] = ticker  # Reverse lookup for database

    return mapped_list, mapped_dict


# Generate symbol mappings for each exchange
for exchange in EXCHANGES:
    EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, BASE_TICKERS)

# EXCHANGES = ["hyperliquid", "binance", "poloniex", "coinbase"]
# BASE_TICKERS = ["BTC_USDT", "ETH_USDT", "OP_USDT"]

# EXCHANGE_SYMBOLS = {}
# REVERSE_SYMBOL_MAP = {exchange: {} for exchange in EXCHANGES}

# for exchange in EXCHANGES:
#     EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, BASE_TICKERS)

print("Hyperliquid Symbols:", EXCHANGE_SYMBOLS["hyperliquid"])
print("Reverse Mapping for Hyperliquid:", REVERSE_SYMBOL_MAP["hyperliquid"])

# for exchange in EXCHANGES:
#     EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, BASE_TICKERS)
#     print(f"[DEBUG] {exchange} symbols →", EXCHANGE_SYMBOLS[exchange])
#       # ✅ Check if hyperliquid is included
