import logging
from crypto_hft.utils.time_utils import iso8601_to_datetime
import asyncpg # type: ignore

class FundingRateInserter:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(**self.db_config)
        logging.info("✅ PostgreSQL connection pool created")

    async def close(self):
        if self.pool:
            await self.pool.close()
            logging.info("🔒 PostgreSQL pool closed")

    async def insert_batch(self, table_name: str, rows: list[dict]):
        if not rows:
            return

        # Use column ordering consistent with your table schema
        columns = [
            "exchange", "symbol", "funding_rate", "funding_time",
            "next_funding_rate", "mark_price", "index_price", "interval", "local_time"
        ]
        batch_data = []

        for row in rows:
            try:
                formatted_row = []
                for col in columns:
                    val = row.get(col)
                    if "time" in col and isinstance(val, str):
                        val = iso8601_to_datetime(val)
                    formatted_row.append(val)
                batch_data.append(tuple(formatted_row))
            except Exception as e:
                logging.warning(f"[⚠️] Skipping invalid row for {table_name}: {e}")

        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(query, batch_data)
            logging.info(f"[✅] Inserted {len(batch_data)} rows into {table_name}")
        except Exception as e:
            logging.error(f"[❌] Failed batch insert for {table_name}: {e}")
