import asyncio
import logging
import time
from crypto_hft.utils.config import Config
from crypto_hft.funding_rate.symbol_manager import get_all_symbols
from crypto_hft.funding_rate.fetch_data import AsyncFundingRateFetcher
from crypto_hft.utils.time_utils import iso8601_to_datetime
import asyncpg

logging.basicConfig(level=logging.INFO)


class FundingRateInserter:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(**self.db_config)
        logging.info("✅ PostgreSQL connection pool created")

    async def insert_batch(self, table_name: str, rows: list[dict]):
        if not rows:
            return

        columns = [
            "exchange", "symbol", "funding_rate", "funding_time",
            "next_funding_rate", "mark_price", "index_price", "interval", "local_time"
        ]

        batch_data = []
        for row in rows:
            try:
                # Convert time strings to datetime
                row["funding_time"] = iso8601_to_datetime(row["funding_time"])
                row["local_time"] = iso8601_to_datetime(row["local_time"])
                batch_data.append(tuple(row[col] for col in columns))
            except Exception as e:
                logging.warning(f"[⚠️] Skipped row due to time parsing error: {e}")

        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(query, batch_data)
            logging.info(f"[✅] Inserted {len(batch_data)} rows into {table_name}")
        except Exception as e:
            logging.error(f"[❌] Failed batch insert for {table_name}: {e}")


# --- Main Loop ---
async def run_every_5_min():
    config = Config()
    db_config = {
        "host": config.postgres_host,
        "port": config.postgres_port,
        "user": config.postgres_user,
        "password": config.postgres_password,
        "database": config.postgres_database
    }

    symbol_metadata = get_all_symbols(
        base_assets=config.target_tokens,
        include_spot=False,
        include_perp=True,
        linear_only=True
    )

    inserter = FundingRateInserter(db_config)
    await inserter.connect()

    fetcher = AsyncFundingRateFetcher()
    await fetcher.load_all_markets()

    while True:
        start = time.time()
        logging.info("🔁 Fetching funding rates...")

        results = await fetcher.fetch_all(symbol_metadata)

        # Insert per exchange
        by_exchange = {"binance": [], "bybit": [], "hyperliquid": []}
        for row in results:
            if "exchange" in row:
                by_exchange[row["exchange"]].append(row)

        for exchange, rows in by_exchange.items():
            table_name = f"funding_{exchange}"
            await inserter.insert_batch(table_name, rows)

        elapsed = time.time() - start
        logging.info(f"✅ Cycle complete in {elapsed:.2f}s — sleeping 5 mins...\n")
        await asyncio.sleep(300)


# --- Run ---
if __name__ == "__main__":
    asyncio.run(run_every_5_min())
