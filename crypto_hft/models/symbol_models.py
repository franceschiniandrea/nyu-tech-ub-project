from dataclasses import dataclass
from typing import Optional

@dataclass
class SymbolMetadata:
    exchange: str
    base: str
    quote: str
    settle: Optional[str]
    symbol_id: str
    unified: str
    normalized: str

    @classmethod
    def from_ccxt(cls, exchange_id: str, m: dict, *, only_perp: bool = False, only_spot: bool = False, allowed_quotes: Optional[set[str]] = None, linear_only: bool = True) -> Optional["SymbolMetadata"]:
        try:
            # Filter by market type if needed
            market_type = m.get("type")
            if only_perp and market_type != "swap":
                return None
            if only_spot and market_type != "spot":
                return None

            # Filter by linear contract type
            if linear_only and market_type == "swap" and m.get("linear") is not True:
                return None

            # Extract components
            base = m.get("base")
            quote = m.get("quote")
            settle = m.get("settle") or quote
            symbol_id = m.get("id")
            unified = m.get("symbol")

            # Filter by quote currency if specified
            if allowed_quotes and quote not in allowed_quotes:
                return None

            # Skip markets that are missing essential fields
            if not base or not quote or not symbol_id or not unified:
                return None

            normalized = f"{base}/{quote}:{settle}"

            return cls(
                exchange=exchange_id,
                base=base,
                quote=quote,
                settle=settle,
                symbol_id=symbol_id,
                unified=unified,
                normalized=normalized
            )
        except Exception:
            return None