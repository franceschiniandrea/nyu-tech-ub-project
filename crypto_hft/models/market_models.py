from typing import Literal, Optional
from datetime import datetime


class NormalizedOrderBook:
    def __init__(
        self,
        exchange: str,
        symbol: str,
        timestamp: datetime,
        local_timestamp: datetime,
        bids: list[tuple[float, float]],  # list of (price, amount)
        asks: list[tuple[float, float]],
        levels: int = 15
    ):
        self.exchange = exchange
        self.symbol = symbol
        self.timestamp = timestamp
        self.local_timestamp = local_timestamp
        self.bids = bids[:levels]
        self.asks = asks[:levels]

    def to_flat_dict(self) -> dict:
        """Flattens the order book for DB insert (e.g. bid_0_px, ask_0_sz, ...)"""
        flat = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "local_timestamp": self.local_timestamp,
        }

        for i, (px, sz) in enumerate(self.bids):
            flat[f"bid_{i}_px"] = px
            flat[f"bid_{i}_sz"] = sz

        for i, (px, sz) in enumerate(self.asks):
            flat[f"ask_{i}_px"] = px
            flat[f"ask_{i}_sz"] = sz

        # Pad remaining levels with None
        for i in range(len(self.bids), 15):
            flat[f"bid_{i}_px"] = None
            flat[f"bid_{i}_sz"] = None
        for i in range(len(self.asks), 15):
            flat[f"ask_{i}_px"] = None
            flat[f"ask_{i}_sz"] = None

        return flat


class NormalizedTrade:
    def __init__(
        self,
        exchange: str,
        symbol: str,
        trade_id: str,
        price: float,
        amount: float,
        side: Literal["buy", "sell"],
        timestamp: datetime,
        local_timestamp: datetime
    ):
        self.exchange = exchange
        self.symbol = symbol
        self.trade_id = trade_id
        self.price = price
        self.amount = amount
        self.side = side
        self.timestamp = timestamp
        self.local_timestamp = local_timestamp

    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": self.price,
            "amount": self.amount,
            "side": self.side,
            "timestamp": self.timestamp,
            "local_timestamp": self.local_timestamp
        }