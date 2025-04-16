import ccxt.pro as ccxtpro
import logging
from .models import ExchangeCurrency, Network
from crypto_hft.utils.config import Config
config = Config()


async def fetch_exchange_metadata(exchange_id: str) -> list[ExchangeCurrency]:
    exchange = getattr(ccxtpro, exchange_id)({'enableRateLimit': True})
    await exchange.load_markets()
    currencies = await exchange.fetch_currencies()

    results = []
    for ccy, data in currencies.items():
        if ccy.upper() not in config.target_tokens:
            continue
        try:
            results.append(ExchangeCurrency(
                exchange=exchange_id,
                ccy=ccy,
                active=data.get("active", True),
                deposit=data.get("info", {}).get("deposit", True),
                withdraw=data.get("info", {}).get("withdraw", True),
                taker_fee=data.get("taker", 0.001),
                maker_fee=data.get("maker", 0.001),
                precision=data.get("precision", {}),
                limits=data.get("limits", {}),
                networks=[Network(
                    id=n.get("id", ""),
                    name=n.get("name", ""),
                    active=n.get("active", True),
                    fee=n.get("fee", 0.0),
                    deposit=n.get("deposit", True),
                    withdraw=n.get("withdraw", True)
                ) for n in data.get("networks", {}).values()]
            ))
        except Exception as e:
            logging.warning(f"[⚠️] Skipping {ccy} on {exchange_id}: {e}")
    await exchange.close()
    return results
