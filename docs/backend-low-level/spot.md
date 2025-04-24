# backend-low-level/spot.md

## Overview
The `spot/` module is responsible for real-time ingestion of **spot market** data using **Tardis Machine** (via WebSocket or file-based replay). It captures **order book snapshots** and **trade events**, normalizes them, and stores them in PostgreSQL using a queue-based batch insertion system.

This module operates independently of CCXT and is optimized for high-volume streaming.

---

## Components

### `main_loop.py`
Main async controller that:
- Initializes the WebSocket consumer
- Sets up PostgreSQL DB connection
- Starts queue processors for order book and trades
- Runs all components concurrently until graceful shutdown

### `websocket.py`
Implements a **Tardis-compatible WebSocket client** for subscribing to and processing real-time messages.

- Connects to live WebSocket feed (e.g., Binance, Coinbase, Hyperliquid)
- Handles `book_snapshot` and `trade` events
- Forwards raw events to `data_processor.py`

### `data_processor.py`
Processes and normalizes incoming raw data from WebSocket:

- Converts Tardis-format snapshots into standard structure
- Pads bids/asks to match 15-level schema
- Converts timestamps to ISO 8601 + Unix UTC
- Pushes normalized data to `order_book_queues` and `trade_queues`

### `queue_manager.py`
Sets up queues used to buffer real-time data for each tracked symbol.

- Initializes:
  - `order_book_queues: dict[str, asyncio.Queue]`
  - `trade_queues: dict[str, asyncio.Queue]`
- Keys use fully normalized lowercase tickers (e.g., `btc_usdt`)

### `db_writer.py`
Handles batch insertion into PostgreSQL.

- Class: `PostgreSQLDatabase`
  - Manages pooled connection
- Class: `QueueProcessor`
  - Reads from `order_book_queues` and `trade_queues`
  - Inserts into tables like `orderbook_<symbol>` and `trade_<symbol>`
  - Fallback to GCS available via optional writer

---

## Execution Flow

1. `main_loop.py` starts:
    - WebSocket + DB initialized
    - `websocket.run()` begins streaming
2. Raw data from WebSocket is sent to `data_processor`
3. Data is normalized and sent to the queue
4. `QueueProcessor` flushes queues into PostgreSQL in batch

---

## Logging & Error Handling

- Logs:
  - `[+] Connected to WebSocket`
  - `[✅] Inserted X rows into orderbook_eth_usdt`
- Handles:
  - WebSocket disconnects with reconnect logic
  - Parsing errors with fallback warnings
  - Insertion errors with retry + GCS (optional)

---

## Pitfalls

- Tardis may not provide every symbol — missing symbols can cause key errors unless handled in queue manager
- Ensure `config.orderbook_levels` matches DB schema exactly (padding logic included)
- Real-time latency not tracked by default — consider adding timestamp drift measurement

---

## To Do

- Add per-exchange stream health tracking
- Dynamically refresh symbol list without restart
- Add compression for GCS fallback (optional)

