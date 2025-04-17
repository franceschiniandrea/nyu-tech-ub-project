This document serves as a complete reference for the PostgreSQL schema used in the crypto HFT system. It outlines all tables, their key columns, datatypes, constraints, and denormalization strategies.

---

## 📁 `orderbook_{symbol}` (e.g., `orderbook_btc_usdt`)
**Description:** Stores 15-level L2 order book snapshots for spot and perpetual contracts.

| Column           | Type      | Description                            |
|------------------|-----------|----------------------------------------|
| exchange         | TEXT      | Exchange name                          |
| symbol           | TEXT      | Normalized symbol (e.g., btc_usdt)     |
| timestamp        | TIMESTAMP | Exchange timestamp (UTC)               |
| local_timestamp  | TIMESTAMP | Time data was received locally         |
| bid_0_sz ...     | FLOAT     | Size of bid at level i (0–14)          |
| bid_0_px ...     | FLOAT     | Price of bid at level i (0–14)         |
| ask_0_sz ...     | FLOAT     | Size of ask at level i (0–14)          |
| ask_0_px ...     | FLOAT     | Price of ask at level i (0–14)         |

✅ **Notes:**
- Each level (0–14) has separate price and size fields.
- Perpetuals tables use prefix `orderbook_perps_{symbol}`.

---

## 📁 `trade_{symbol}` (e.g., `trade_btc_usdt`)
**Description:** Stores executed trade data.

| Column          | Type      | Description                            |
|------------------|-----------|----------------------------------------|
| exchange        | TEXT      | Exchange name                          |
| symbol          | TEXT      | Normalized symbol                      |
| trade_id        | TEXT      | Unique trade ID from the exchange      |
| price           | FLOAT     | Trade price                            |
| amount          | FLOAT     | Trade size                             |
| side            | TEXT      | 'buy' or 'sell'                        |
| timestamp       | TIMESTAMP | Exchange-provided timestamp            |
| local_timestamp | TIMESTAMP | Timestamp the trade was received       |

✅ **Notes:**
- Perpetuals use prefix `trade_perps_{symbol}`.

---

## 📁 `funding_{exchange}` (e.g., `funding_binance`)
**Description:** Funding rate snapshots for perpetual contracts.

| Column            | Type      | Description                            |
|--------------------|-----------|----------------------------------------|
| exchange           | TEXT      | Exchange name                          |
| symbol             | TEXT      | Normalized symbol                      |
| funding_rate       | FLOAT     | Most recent funding rate               |
| funding_time       | TIMESTAMP | Time at which funding was applied      |
| next_funding_rate  | FLOAT     | Projected next funding rate (optional) |
| mark_price         | FLOAT     | Mark price at funding time             |
| index_price        | FLOAT     | Index price at funding time            |
| interval           | TEXT      | Interval string (e.g., '8h')           |
| local_time         | TIMESTAMP | Time snapshot was collected            |

---

## 📁 `currency_snapshots`
**Description:** Snapshot of metadata for a given currency on a given exchange.

| Column       | Type      | Description                               |
|--------------|-----------|-------------------------------------------|
| snapshot_id  | SERIAL PK | Unique ID                                 |
| exchange     | TEXT      | Exchange name                             |
| ccy          | TEXT      | Currency code (e.g., BTC, ETH)            |
| active       | BOOLEAN   | Whether trading is active                 |
| deposit      | BOOLEAN   | Whether deposits are supported            |
| withdraw     | BOOLEAN   | Whether withdrawals are supported         |
| taker_fee    | FLOAT     | Taker fee                                 |
| maker_fee    | FLOAT     | Maker fee                                 |
| precision    | JSONB     | Price/amount precision                    |
| limits       | JSONB     | Trading limits                            |
| networks     | JSONB     | List of supported blockchain networks     |
| timestamp    | TIMESTAMP | Time the snapshot was collected           |

---

## 📁 `currency_changes`
**Description:** Logs which fields changed between two currency snapshots.

| Column               | Type      | Description                              |
|----------------------|-----------|------------------------------------------|
| change_id            | SERIAL PK | Unique change log ID                     |
| exchange             | TEXT      | Exchange name                            |
| ccy                  | TEXT      | Currency code                            |
| changed_fields       | TEXT[]    | List of fields that changed              |
| previous_snapshot_id | INTEGER   | FK to old `currency_snapshots.snapshot_id` |
| new_snapshot_id      | INTEGER   | FK to new `currency_snapshots.snapshot_id` |

✅ **Notes:**
- Uses `currency_snapshots.snapshot_id` as a foreign key.
- Only stores fields that changed (e.g., `taker_fee`, `networks`).

---

## 🔗 Design Notes

- All `orderbook_` and `trade_` tables are symbol-specific and created dynamically.
- Currency metadata is periodically snapshotted and compared to detect changes.
- JSONB is used for fields that vary by network or have nested structure.
- Normalized symbols use format: `BASE/QUOTE:SETTLE` → `btc_usdt`, `eth_usdc:usdc`.
