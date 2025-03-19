import numpy as np
import math
import logging
from crypto_hft.utils.config import Config

# Load configuration
config = Config()
orderbook_levels = config.orderbook_levels  

def process_trade_data(trade :dict, received_symbol: str) ->dict:
    """
    Processes trade messages to ensure consistency with the database schema.
    """
    try:
        processed_data = {
            "exchange": trade["exchange"],
            "symbol": received_symbol,
            "trade_id": trade["id"],  
            "price": trade["price"],
            "amount": trade["amount"],  
            "side": trade["side"],
            "timestamp": trade["timestamp"],
            "local_timestamp": trade.get("localTimestamp", None) 
        }

        if processed_data["side"] is None:
            logging.warning(f"⚠️ Trade missing 'side' field: {trade}")

        return processed_data

    except KeyError as e:
        logging.error(f"[PROCESSING ERROR] Missing expected key in trade data: {e}")
        return None
    except Exception as e:
        logging.error(f"[PROCESSING ERROR] Unexpected error processing trade: {e}")
        return None
    

def process_order_book_data(order_book:dict, received_symbol:str) ->dict:
    """
    Processes order book data, ensuring correct ordering and MySQL compatibility.
    """
    try:
        bids = order_book.get("bids", [])[:orderbook_levels]
        asks = order_book.get("asks", [])[:orderbook_levels]

        # Initialize arrays filled with NaN
        bid_prices = np.full(orderbook_levels, np.nan)
        bid_sizes = np.full(orderbook_levels, np.nan)
        ask_prices = np.full(orderbook_levels, np.nan)
        ask_sizes = np.full(orderbook_levels, np.nan)

        # Fill available data
        bid_prices[:len(bids)] = [bid["price"] for bid in bids]
        bid_sizes[:len(bids)] = [bid["amount"] for bid in bids]
        ask_prices[:len(asks)] = [ask["price"] for ask in asks]
        ask_sizes[:len(asks)] = [ask["amount"] for ask in asks]

        # ✅ Create the processed order book dictionary in correct MySQL order
        processed_order_book = {
            "exchange": order_book["exchange"],
            "symbol": received_symbol,
            "timestamp": order_book["timestamp"],
            "local_timestamp": order_book.get("localTimestamp", None),
        }

        # ✅ replace Nan with None for db write compatibility
        processed_order_book.update({
            key: None if math.isnan(value) else float(value)
            for i in range(orderbook_levels)
            for key, value in zip(
                (f"bid_{i}_sz", f"bid_{i}_px", f"ask_{i}_sz", f"ask_{i}_px"),
                (bid_sizes[i], bid_prices[i], ask_sizes[i], ask_prices[i])
            )
        })

        return processed_order_book

    except Exception as e:
        logging.error(f"[ERROR] Failed to process order book data: {order_book} - {e}")
        return None