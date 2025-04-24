import ccxt # type: ignore 

# --- Exchange Loader ---
def load_exchange(name: str, spot: bool = False):
    exchange_map = {
        "binance": ccxt.binance,
        "bybit": ccxt.bybit,
        "poloniex": ccxt.poloniex,
        "poloniexperpetuals": ccxt.poloniexfutures,
        "hyperliquid": ccxt.hyperliquid
    }
 
    if name not in exchange_map:
        raise ValueError(f"Exchange '{name}' is not supported.")

    opts = {'options': {'defaultType': 'spot' if spot else 'future'}}
    return exchange_map[name](opts)


# --- Symbol Extractor ---
def get_symbol_metadata(exchange, only_perps=True, base_assets=None, linear_only=True):
    exchange.load_markets()
    symbols = []

    for m in exchange.markets.values():
        if only_perps and m.get("type") != "swap":
            continue
        if not only_perps and m.get("type") != "spot":
            continue
        if base_assets and m.get("base") not in base_assets:
            continue
        if linear_only and m.get("linear") is not True:
            continue

        unified = m["symbol"]
        id_ = m["id"]
        base = m.get("base")
        quote = m.get("quote")
        settle = m.get("settle") or quote  # fallback if settle not explicitly given
        normalized = f"{base}/{quote}:{settle}"

        symbols.append({
            "unified": unified,
            "id": id_,
            "normalized": normalized,
            "base": base,
            "quote": quote,
            "settle": settle
        })

    return symbols


# --- Main Symbol Aggregator ---
def get_all_symbols(base_assets=None, include_spot=True, include_perp=True, linear_only=True):
    exchanges = [
        ("binance", False),
        ("bybit", False),
        ("poloniex", True),
        ("poloniexperpetuals", False),
        ("hyperliquid", False)
    ]

    all_symbols = []

    for name, is_spot in exchanges:
        if is_spot and not include_spot:
            continue
        if not is_spot and not include_perp:
            continue

        try:
            ex = load_exchange(name, spot=is_spot)
            symbols = get_symbol_metadata(
                ex,
                only_perps=not is_spot,
                base_assets=base_assets,
                linear_only=linear_only
            )
            for s in symbols:
                s["exchange"] = name
            all_symbols.extend(symbols)
        except Exception as e:
            all_symbols.append({"exchange": name, "error": str(e)})

    return all_symbols


if __name__ == "__main__":
    import pprint
    tokens = ["BTC", "ETH", "DOGE", "SOL"]
    symbol_data = get_all_symbols(
        base_assets=tokens,
        include_spot=False,
        include_perp=True,
        linear_only=True
    )
    pprint.pprint(symbol_data)
