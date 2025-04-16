# Utils Module – Backend Low-Level Documentation

## Purpose

The `utils/` folder contains shared configuration, logging, and helper utilities that support both spot and perpetual pipelines.

---

## Components

### `config.py`
- **Purpose**: Loads environment variables and constants.
- **Key Features**:
  - Loads `.env` secrets on initialization.
  - Defines constants like `orderbook_levels`, batching thresholds, logging levels.
  - Exposes `Config` object with all secrets and config values.

### `symbol_mapper.py`
- **Purpose**: Handles normalization and reverse mapping of symbols across exchanges.
- **Key Functions**:
  - `map_symbols`: Maps standard `BASE_QUOTE` format to exchange-specific formats.
  - `reverse_map_symbol`: Converts exchange symbol format back to standard format.
  - `get_valid_symbol`: Returns proper unified symbol for a given exchange/token.

### `time_utils.py`
- **Purpose**: Timestamp parsing and conversion.
- **Functions**:
  - `iso8601_to_unix`: Converts ISO8601 string to UNIX timestamp.
  - `unix_to_mysql_datetime`: Converts UNIX timestamp to MySQL datetime.
  - `parse_timestamp`: Wrapper to flexibly handle both ISO and UNIX inputs.

### `logging.py`
- **Purpose**: Centralizes logging setup using `loguru`.
- **Features**:
  - File, console, and optional Telegram logging.
  - Custom formatting with timestamp and emoji indicators.

---

## Future Improvements

- Add async retry utilities for failed API calls.
- Consolidate `symbol_mapper` logic with `symbol_manager`.
- Add unit tests for `time_utils`.
