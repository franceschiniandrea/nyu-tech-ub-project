import asyncio
import datetime
import ccxt.async_support as ccxt

from crypto_hft.funding_rate.symbol_manager import get_all_symbols
from crypto_hft.utils.config import Config


def normalize(data, exchange: str, symbol: str) -> dict:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "funding_rate": data.get("fundingRate") or data.get("funding"),
        "funding_time": data.get("fundingDatetime"),
        "next_funding_rate": data.get("nextFundingRate"),
        "mark_price": data.get("markPrice"),
        "index_price": data.get("indexPrice"),
        "interval": data.get("interval") or ("1h" if exchange == "hyperliquid" else "8h"),
        "local_time": datetime.datetime.now(datetime.UTC).isoformat()
    }


class AsyncFundingRateFetcher:
    def __init__(self):
        self.binance = ccxt.binance({'options': {'defaultType': 'future'}})
        self.bybit = ccxt.bybit({'options': {'defaultType': 'future'}})
        self.hyperliquid = ccxt.hyperliquid()

    async def load_all_markets(self):
        await asyncio.gather(
            self.binance.load_markets(),
            self.bybit.load_markets(),
            self.hyperliquid.load_markets()
        )

    async def fetch_binance(self, symbols: list[dict]) -> list[dict]:
        tasks, results = [], []
        binance_symbols = [s for s in symbols if s["exchange"] == "binance"]

        for s in binance_symbols:
            tasks.append(self.binance.fetchFundingRate(s["unified"]))

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for s, r in zip(binance_symbols, responses):
            if isinstance(r, Exception):
                results.append({"exchange": "binance", "symbol": s["normalized"], "error": str(r)})
            else:
                results.append(normalize(r, "binance", s["normalized"]))
        return results

    async def fetch_bybit(self, symbols: list[dict]) -> list[dict]:
        tasks, results = [], []
        bybit_symbols = [s for s in symbols if s["exchange"] == "bybit" and "PERP" in s["id"]]

        for s in bybit_symbols:
            tasks.append(self.bybit.fetchFundingRate(s["id"]))

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for s, r in zip(bybit_symbols, responses):
            if isinstance(r, Exception):
                results.append({"exchange": "bybit", "symbol": s["normalized"], "error": str(r)})
            else:
                results.append(normalize(r, "bybit", s["normalized"]))
        return results

    async def fetch_hyperliquid(self, symbols: list[dict]) -> list[dict]:
        results = []
        try:
            data = await self.hyperliquid.fetchFundingRates()
            for rate in data.values():
                match = next(
                    (s for s in symbols if s["exchange"] == "hyperliquid" and s["unified"] == rate["symbol"]),
                    None
                )
                if match:
                    results.append(normalize(rate, "hyperliquid", match["normalized"]))
        except Exception as e:
            results.append({"exchange": "hyperliquid", "symbol": "ALL", "error": str(e)})
        return results

    async def fetch_all(self, symbols: list[dict]) -> list[dict]:
        return sum(await asyncio.gather(
            self.fetch_binance(symbols),
            self.fetch_bybit(symbols),
            self.fetch_hyperliquid(symbols)
        ), [])

    async def close(self):
        await self.binance.close()
        await self.bybit.close()
        await self.hyperliquid.close()


# # --- Run Manually ---
# if __name__ == "__main__":
#     import pprint

#     async def main():
#         config = Config()
#         symbols = get_all_symbols(
#             base_assets=config.target_tokens,
#             include_spot=False,
#             include_perp=True,
#             linear_only=True
#         )

#         fetcher = AsyncFundingRateFetcher()
#         await fetcher.load_all_markets()
#         results = await fetcher.fetch_all(symbols)
#         await fetcher.close()

#         pprint.pprint(results)

#     asyncio.run(main())
