from crypto_hft.utils.config import Config

config = Config()

EXCHANGE_SYMBOLS = {}
REVERSE_SYMBOL_MAP: dict[str, dict] = {exchange: {} for exchange in config.exchanges}

EXCHANGE_MAPPING_RULES = {
    "hyperliquid": lambda base, quote: base.lower(),  
    "binanceus": lambda base, quote: f"{base}{quote}".lower(), 
    "binance": lambda base, quote: f"{base}{quote}".lower(),     
    "poloniex": lambda base, quote: f"{base}_{quote}",          
    "coinbase": lambda base, quote: f"{base.upper()}-{quote.upper()}" 
}

# Rules to map exchange-specific symbol format back to standard format
EXCHANGE_OUTPUT_MAPPING_RULES = {
    "hyperliquid": lambda symbol: f"{symbol.upper()}_USDT",  # e.g., "BTC_USDT"
    "binanceus": lambda symbol: f"{symbol[:-4].upper()}_USDT" if symbol.endswith("usdt") else symbol,
    "binance": lambda symbol: f"{symbol[:-4].upper()}_USDT" if symbol.endswith("usdt") else symbol,
    "poloniex": lambda symbol: symbol,  # Already in correct format
    "coinbase": lambda symbol: symbol.replace("-", "_")
}

def map_symbols(exchange, tickers):
    """
    Maps tickers to the format required by each exchange for WebSocket requests.
    Also creates a reverse mapping to standardize received symbols.
    
    Args:
        exchange (str): Exchange name.
        tickers (list[str]): List of tickers in 'BASE_QUOTE' format (e.g., "BTC_USDT").
    
    Returns:
        tuple: (mapped_list, mapped_dict)
            mapped_list: Symbols formatted for WebSocket requests.
            mapped_dict: Dictionary mapping exchange-specific symbols to standard tickers.
    """
    mapped_list = []
    mapped_dict = {}

    for ticker in tickers:
        base, quote = ticker.split("_")
        base, quote = base.lower(), quote.lower()

        # Use custom mapping rule if available, else default to 'base_quote'
        mapped = EXCHANGE_MAPPING_RULES.get(exchange, lambda b, q: f"{b}_{q}")(base, quote)

        mapped_list.append(mapped)
        mapped_dict[mapped.upper()] = ticker  

    return mapped_list, mapped_dict

def reverse_map_symbol(exchange, symbol):
    """
    Converts exchange-specific symbol format back to the standard 'BASE_QUOTE' format.
    """
    symbol_upper = symbol.upper()  # 🔹 Convert input to uppercase for lookup
    standardized_symbol = EXCHANGE_OUTPUT_MAPPING_RULES.get(exchange, lambda s: s)(symbol_upper)

    return standardized_symbol


for exchange in config.exchanges:
    EXCHANGE_SYMBOLS[exchange], REVERSE_SYMBOL_MAP[exchange] = map_symbols(exchange, config.base_tickers)


# print(EXCHANGE_SYMBOLS)
# print(REVERSE_SYMBOL_MAP)