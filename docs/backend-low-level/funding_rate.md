# Funding Rate Module – Backend Low-Level Documentation

## Purpose

The funding rate module tracks and stores periodic funding rate data for perpetual contracts from multiple exchanges. It normalizes and stores funding rate, mark price, index price, and timing fields into PostgreSQL.

---

## Components

### `fetch_data.py`
- **Purpose**: Uses `ccxt` to asynchronously fetch funding rate data.
- **Key Functions**:
  - `AsyncFundingRateFetcher`: Class containing methods to fetch funding data from Binance, Bybit, and Hyperliquid.
  - `fetch_all`: Gathers normalized data from all exchanges into one list.
  - `normalize`: Converts raw exchange data into unified schema.

### `funding_fetch_loop.py`
- **Purpose**: Background task that fetches and stores funding rates every 5 minutes.
- **Logic Flow**:
  1. Load target token metadata using `get_all_symbols`.
  2. Fetch live funding data via `AsyncFundingRateFetcher`.
  3. Group results by exchange.
  4. Use `FundingRateInserter` to batch insert into PostgreSQL tables.

### `insert_funding_data.py`
- **Purpose**: Optional module to insert one-off batches of funding rate data.
- **Functions**:
  - `insert_batch`: Formats and inserts normalized funding data into PostgreSQL.

### `symbol_manager.py`
- **Purpose**: Extracts and normalizes perpetual symbols from each exchange using `ccxt`.
- **Highlights**:
  - Supports `binance`, `bybit`, `poloniex`, and `hyperliquid`.
  - Filters by quote currency and linearity.

---

## Storage Schema

- Each exchange has a table named `funding_<exchange>`.
- Fields: `exchange`, `symbol`, `funding_rate`, `funding_time`, `next_funding_rate`, `mark_price`, `index_price`, `interval`, `local_time`.

---

## Error Handling

- Exchange-specific try/catch blocks around all API calls.
- Normalization failures log warnings but don’t halt execution.
- Retry logic not yet implemented but can be added in future.

---

## Improvements
- Add retry logic for intermittent network/API failures.
- Deduplicate code between fetchers and funding insert logic. (create a normalization class instead)
