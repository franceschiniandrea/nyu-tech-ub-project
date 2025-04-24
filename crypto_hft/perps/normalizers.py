# normalizers.py

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

ORDERBOOK_LEVELS = 15

def normalize_symbol(symbol: str) -> str:
    """
    Normalize a CCXT symbol like 'BTC/USDT:USDT' to 'btcusdt'.
    This function is used consistently across queue creation and data normalization.
    """
    if ":" in symbol:
        symbol = symbol.split(":")[0]  # Strip contract postfix like ':USDT'
    return symbol.replace("/", "").lower()  # Remove slash and make lowercase


def normalize_order_book(exchange_id: str, raw: dict) -> Dict[str, Any]:
    """
    Normalize the raw order book data from exchange.
    """
    try:
        symbol_raw = raw.get("symbol", "")
        normalized_symbol = normalize_symbol(symbol_raw)

        return {
            "exchange": exchange_id,
            "symbol": normalized_symbol,
            "timestamp": raw.get("timestamp"),
            "local_timestamp": raw.get("datetime"),
            "bids": raw.get("bids", [])[:ORDERBOOK_LEVELS],
            "asks": raw.get("asks", [])[:ORDERBOOK_LEVELS],
        }
    except Exception as e:
        logger.warning(f"[ORDERBOOK] Normalization error for {exchange_id} {raw.get('symbol')}: {e}")
        return {}


def normalize_trade(exchange_id: str, raw: dict) -> Dict[str, Any]:
    """
    Normalize the raw trade data from exchange.
    """
    try:
        symbol_raw = raw.get("symbol", "")
        normalized_symbol = normalize_symbol(symbol_raw)

        return {
            "exchange": exchange_id,
            "symbol": normalized_symbol,
            "trade_id": str(raw.get("id")),
            "price": float(raw.get("price")),
            "amount": float(raw.get("amount")),
            "side": raw.get("side"),
            "timestamp": raw.get("timestamp"),
            "local_timestamp": raw.get("datetime"),
        }
    except Exception as e:
        logger.warning(f"[TRADES] Normalization error for {exchange_id} {raw.get('symbol')}: {e}")
        return {}
