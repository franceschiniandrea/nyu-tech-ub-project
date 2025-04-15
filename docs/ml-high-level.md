# Machine Learning Systems Overview

## Current Status
This project currently focuses on building the **high-frequency trading data infrastructure** and does not yet implement machine learning models. The collected market data serves as the foundation for potential future ML applications.

## Future ML Integration Plan

### Potential Use Cases
| Use Case | Description | Data Requirements |
|----------|-------------|-------------------|
| Order Book Imbalance Prediction | Predict short-term price movements | Order book snapshots (15 levels) |
| Trade Signature Analysis | Detect unusual trading patterns | Raw trade ticks with microsecond timestamps |
| Latency Arbitrage Detection | Identify cross-exchange opportunities | Multi-exchange order book data |

### Data Pipeline Readiness
```mermaid
graph LR
    A[Raw WebSocket Data] --> B[MySQL Database]
    B --> C[Feature Engineering]
    C --> D[Model Training]
    D --> E[Real-time Inference]