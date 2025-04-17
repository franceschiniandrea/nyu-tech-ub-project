import ccxt.async_support as ccxt
import logging
from crypto_hft.utils.config import Config
from crypto_hft.metadata.models import ExchangeCurrency, Network

config = Config()

async def fetch_exchange_metadata(exchange_id: str) -> list[ExchangeCurrency]:
    try:
        logging.info(f"[🔍] Fetching metadata from {exchange_id}...")

        exchange = ccxt.binance({
            'enableRateLimit': True,
            'apiKey': config.binance_api_key,
            'secret': config.binance_secret,
        }) if exchange_id == "binance" else getattr(ccxt, exchange_id)({'enableRateLimit': True})

        await exchange.load_markets()
        currencies = await exchange.fetch_currencies()

        if not isinstance(currencies, dict):
            logging.warning(f"[⚠️] Invalid currencies format from {exchange_id}.")
            await exchange.close()
            return []

        results = []
        matched = 0

        for ccy, data in currencies.items():
            if ccy.upper() not in config.target_tokens:
                continue
            matched += 1

            info_data = data["info"][0].get(ccy, {}) if isinstance(data.get("info"), list) else data.get("info", {})
            networks = []

            for net_id, net_data in data.get("networks", {}).items():
                try:
                    network = Network.from_dict(net_data)
                    networks.append(network)
                except KeyError as e:
                    logging.warning(f"[⚠️] Skipping incomplete network on {ccy}@{exchange_id}: {e}")
                    # Still include it, but flagged as incomplete
                    networks.append(Network(
                        id=net_data.get("id", net_id),
                        name=net_data.get("network", net_id),
                        active=net_data.get("active", False),
                        fee=net_data.get("fee", 0.0),
                        deposit=net_data.get("deposit", False),
                        withdraw=net_data.get("withdraw", False),
                        withdrawal_limits=Limit("withdraw", 0, 0),
                        deposit_limits=Limit("deposit", 0, 0),
                        is_complete=False
                    ))

            results.append(ExchangeCurrency(
                exchange=exchange_id,
                ccy=ccy,
                active=data["active"],
                deposit=info_data["walletDepositState"] if exchange_id == "poloniex" else info_data["deposit"],
                withdraw=info_data["walletWithdrawalState"] if exchange_id == "poloniex" else info_data["withdraw"],
                taker_fee=data["taker"],
                maker_fee=data["maker"],
                precision=data["precision"],
                limits=data["limits"],
                networks=networks
            ))

        logging.info(f"[✅] {exchange_id}: Matched {matched} target currencies")
        await exchange.close()
        return results

    except Exception as e:
        logging.error(f"[❌] Failed to fetch or process {exchange_id}: {e}")
        return []