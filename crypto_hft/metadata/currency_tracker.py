import asyncio
import logging
import time
from crypto_hft.utils.config import Config
from crypto_hft.metadata.fetcher import fetch_exchange_metadata
from crypto_hft.metadata.inserter import CurrencyMetadataInserter
from crypto_hft.metadata.differ import compare_snapshots

config = Config()
EXCHANGES = ["binance", "poloniex"]

async def main():
    config = Config()
    db = CurrencyMetadataInserter({
        "host": config.postgres_host,
        "port": config.postgres_port,
        "user": config.postgres_user,
        "password": config.postgres_password,
        "database": config.postgres_database
    })
    await db.connect()

    while True:
        logging.info("\n🔁 Starting currency metadata cycle...")
        for exchange_id in EXCHANGES:
            try:
                currencies = await fetch_exchange_metadata(exchange_id)
                for ccy in currencies:
                    old = await db.get_latest_snapshot(ccy.exchange, ccy.ccy)
                    new_snapshot_id = await db.insert_snapshot(ccy)
                    if old:
                        prev_snapshot_id = old["snapshot_id"]
                        changed = compare_snapshots(old, ccy)
                        if changed:
                            await db.insert_change_log(
                                ccy.exchange, ccy.ccy, changed,
                                prev_snapshot_id, new_snapshot_id
                            )
                            logging.info(f"🔄 Change detected: {ccy.exchange}:{ccy.ccy} – {changed}")
            except Exception as e:
                logging.error(f"[❌] Failed to fetch or process {exchange_id}: {e}")

        logging.info("✅ Metadata poll complete. Sleeping 15 minutes.")
        await asyncio.sleep(900)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
