# Metadata Component

> Handles the periodic fetching, normalization, and storage of **currency metadata** from multiple exchanges (Binance, Poloniex, Bybit). Supports snapshotting and change detection.

---

## 📁 Files & Purpose

| File | Description |
|------|-------------|
| `currency_tracker.py` | Entrypoint script that schedules and orchestrates metadata fetching. |
| `fetcher.py` | Asynchronously fetches metadata from each exchange via `ccxt`. |
| `insert.py` | Handles insertion of metadata into PostgreSQL and logging of field-level changes. |
| `models.py` | Dataclass definitions for `ExchangeCurrency`, `Network`, and `Limit`. Used to normalize and structure metadata. |

---

## ⚙️ Main Responsibilities

- Load and filter currency metadata from supported exchanges
- Normalize and structure metadata using consistent dataclasses
- Insert periodic snapshots into `currency_snapshots` table
- Compare with latest snapshot to detect field-level changes
- Log changes into `currency_changes` for auditability

---

## 🔁 Logic Flow

    A[Start Schedule] --> B[Fetch metadata from all exchanges]
    B --> C[Filter by TARGET_TOKENS]
    C --> D[Normalize with ExchangeCurrency model]
    D --> E[Insert snapshot into PostgreSQL]
    E --> F[Compare with previous snapshot]
    F --> G[Log changes to currency_changes]
    G --> H[Sleep and repeat every 15 min]
