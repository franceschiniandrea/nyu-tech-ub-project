# Crypto Price Chart Streamlit App

This Streamlit application displays price charts for crypto pairs using trade data from the PostgreSQL database.

## How to Run
2. Ensure your `.env` file has the correct database credentials
3. Run the Streamlit app with:

```bash
streamlit run crypto_hft/streamlit/price_chart.py
```

## Configuration

The app uses the database credentials from your `.env` file. Make sure these are set correctly:

- `postgres_host`
- `postgres_user`
- `postgres_password`
- `postgres_database`
