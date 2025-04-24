# 🔭 High-Level Overview

Welcome to the crypto-HFT project! This document outlines all major components, their purpose, and links to detailed technical documentation.

---

## 📂 Spot Market Pipeline (`spot/`)
Real-time L2 order book and trade streaming from Tardis into PostgreSQL.

- `main_loop.py`: [Execution loop for spot]
- `websocket.py`: [WebSocket listener]
- `db_writer.py`: [PostgreSQL batch writer]
- `queue_manager.py`: [Queue setup]
- `data_processor.py`: [Data normalization]

---

## 📂 Perpetuals Pipeline (`perps/`)
CCXT-based streaming + failover support for perpetual futures.

- `launcher_script.py`: [Main runner]
- `streamer.py`: [CCXT WebSocket wrapper]
- `create_queue.py`: [Queue init for perps]
- `normalizers.py`: [Order book/trade normalization]
- `postgres_utils.py`: [DB insert + GCS fallback]
- `gcs_fallback_writer.py`: [GCS fallback uploader]

---

## 📂 Metadata Trackers (`metadata/`)
Pollers for funding rates, currency metadata, and symbol metadata.

- `currency_tracker.py`: [Currency polling loop]
- `funding_fetch_loop.py`: [Funding rate polling loop]
- `symbol_manager.py`: [Symbol mapper]

---

## 📂 Shared Utilities (`utils/`)
Cross-module config, logging, time utils, etc.

- `config.py`: [Global config loader]
- `time_utils.py`: [Timestamp helpers]
- `symbol_mapper.py`: [Exchange symbol mappings]

---

## 🧱 Models (`models/`)
Data structures used for normalized metadata.

- `symbol_models.py`: [SymbolMetadata class]
- `currency_models.py`: [ExchangeCurrency class]
