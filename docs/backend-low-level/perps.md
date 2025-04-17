# backend-low-level/perps.md

## Overview
The `perps/` module handles real-time data ingestion and storage for **perpetual futures markets** using CCXT (or raw APIs where needed). It processes **order book** and **trade** data, normalizes it, queues it, and batch inserts it into a PostgreSQL database. It also supports GCS fallback in case of DB failure.

---

## Components

### `launcher_script.py`
**Entrypoint** for running all core components (streamer, order book writer, trade writer) concurrently with signal-based graceful shutdown.

- Initializes PostgreSQL connection and queue processor
- Runs `streamer.main()` and batch insert coroutines
- Handles `SIGINT` and `SIGTERM` for graceful shutdown
- Maintains global `shutdown_event` and task cancellation

### `streamer.py`
Handles the real-time data streaming using **CCXT Pro** or other async clients.

- Watches top-level order book (`watch_order_book`) and trades (`watch_trades`)
- Normalizes the data using `normalizers.py`
- Pushes results to symbol-specific `order_book_queues_perps` and `trade_queues_perps`

### `create_queue.py`
Initializes per-symbol async queues for order book and trade data.

- Iterates over all `TARGET_TOKENS` from `config.py`
- Normalizes symbols for exchange compatibility
- Builds:
  - `order_book_queues_perps: dict[str, asyncio.Queue]`
  - `trade_queues_perps: dict[str, asyncio.Queue]`

### `normalizers.py`
Standardizes raw data structure across exchanges.

- `normalize_symbol(symbol: str)` → `str`
- `normalize_order_book(exchange_id, raw)`
- `normalize_trade(exchange_id, raw)`

Also includes padded-level logic for 15-level bid/ask order books.

### `postgres_utils.py`
Implements batch-insertion logic with PostgreSQL connection pooling.

- Class: `PostgreSQLDatabase`
  - `.connect()`, `.close()`, `.insert_batch()`
- Class: `QueueProcessor`
  - Processes per-symbol queues in parallel
  - Batches inserts to PostgreSQL
  - Falls back to `GCSFallbackWriter` on failure

### `gcs_fallback_writer.py`
Handles backup storage of failed batches to **Google Cloud Storage** in Parquet format using DuckDB.

- Writes to partitioned Parquet files per symbol
- Uploads to specified GCS bucket

---

## Execution Flow

1. `launcher_script.py` starts:
    - DB connection
    - Streamer task
    - Queue processors
2. `streamer.py` listens to real-time updates
3. Data is normalized and pushed to queues
4. `postgres_utils.QueueProcessor` reads queues in batch
5. Data is inserted into PostgreSQL or offloaded to GCS

---

## Logging & Error Handling

- Catches WebSocket disconnects and restarts them
- Fallbacks log `symbol`, `error`, and queue state
- Metrics (e.g., rows inserted) shown in info logs

---

## Pitfalls

- Ensure symbol normalization consistency across queues and streamer
- GCS credentials must be properly configured or fallback will silently fail
- PostgreSQL batch insert assumes schema matches normalized structure
- Exchange rate limits can affect `watch_*` subscriptions

---

## To Do

- Add dynamic symbol discovery from CCXT metadata
- Add Prometheus-compatible metrics
- Add retry logic for failed GCS uploads

