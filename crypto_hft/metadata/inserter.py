import asyncpg
import logging
from .models import ExchangeCurrency

class CurrencyMetadataInserter:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.pool: asyncpg.pool.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(**self.db_config)
        logging.info("✅ PostgreSQL connection established.")

    async def insert_snapshot(self, obj: ExchangeCurrency) -> int:
        async with self.pool.acquire() as conn:
            query = """
                INSERT INTO currency_snapshots (
                    exchange, ccy, active, deposit, withdraw,
                    taker_fee, maker_fee, precision, limits, networks, timestamp
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                RETURNING snapshot_id
            """
            row = await conn.fetchrow(query, *obj.to_row())
            return row["snapshot_id"]

    async def get_latest_snapshot(self, exchange: str, ccy: str) -> dict:
        async with self.pool.acquire() as conn:
            query = """
                SELECT snapshot_id, active, deposit, withdraw, precision, limits, networks
                FROM currency_snapshots
                WHERE exchange = $1 AND ccy = $2
                ORDER BY timestamp DESC
                LIMIT 1
            """
            row = await conn.fetchrow(query, exchange, ccy)
            return dict(row) if row else {}

    async def insert_change_log(self, exchange: str, ccy: str, fields: list[str],
                                prev_id: int, new_id: int):
        async with self.pool.acquire() as conn:
            query = """
                INSERT INTO currency_changes (
                    exchange, ccy, changed_fields,
                    previous_snapshot_id, new_snapshot_id
                ) VALUES ($1, $2, $3, $4, $5)
            """
            await conn.execute(query, exchange, ccy, fields, prev_id, new_id)
