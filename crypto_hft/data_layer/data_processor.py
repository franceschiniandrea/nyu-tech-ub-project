import logging

# Enable logging
LOG_PROCESSED_MESSAGES = False  # Set to False to disable processed data logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_processor.log"),  # Save logs to a file
        logging.StreamHandler()  # Also print logs to the console
    ]
)

def process_trade_data(trade):
    """
    Processes trade messages to ensure consistency with the database schema.
    """
    try:
        processed_data = {
            "exchange": trade["exchange"],
            "symbol": trade["symbol"],
            "trade_id": trade["id"],  # ✅ Extract trade_id directly
            "price": trade["price"],
            "amount": trade['amount'],
            "side": trade['side'],  # ✅ Default to "buy" if missing
            "timestamp": trade["timestamp"],
            "local_timestamp": trade.get("localTimestamp", None)  # ✅ Ensure local timestamp
        }

        return processed_data

    except KeyError as e:
        logging.error(f"[PROCESSING ERROR] Missing expected key in trade data: {e}")
        return None
    except Exception as e:
        logging.error(f"[PROCESSING ERROR] Unexpected error processing trade: {e}")
        return None
    

def process_order_book_data(order_book):
    """
    Processes order book snapshot data.

    Args:
        order_book (dict): Order book snapshot from WebSocket.

    Returns:
        dict: Processed order book data structured for database insertion.
    """
    try:
        bids = order_book.get("bids", [])[:10]  # Take top 10 bids
        asks = order_book.get("asks", [])[:10]  # Take top 10 asks

        # Extract bid and ask levels, filling missing values with None
        # todo fill with np.nan instead of None
        bid_prices = [bid["price"] for bid in bids] + [None] * (10 - len(bids))
        bid_sizes = [bid["amount"] for bid in bids] + [None] * (10 - len(bids))
        ask_prices = [ask["price"] for ask in asks] + [None] * (10 - len(asks))
        ask_sizes = [ask["amount"] for ask in asks] + [None] * (10 - len(asks))
        
        processed_order_book = {
            "exchange": order_book.get("exchange"),
            "symbol": order_book.get("symbol"),
            "timestamp": order_book.get("timestamp"),
            "local_timestamp": order_book.get("localTimestamp"),
            # todo this should be automated based on the number of levels
            "bid_0_px": bid_prices[0], "bid_0_sz": bid_sizes[0],
            "bid_1_px": bid_prices[1], "bid_1_sz": bid_sizes[1],
            "bid_2_px": bid_prices[2], "bid_2_sz": bid_sizes[2],
            "bid_3_px": bid_prices[3], "bid_3_sz": bid_sizes[3],
            "bid_4_px": bid_prices[4], "bid_4_sz": bid_sizes[4],
            "bid_5_px": bid_prices[5], "bid_5_sz": bid_sizes[5],
            "bid_6_px": bid_prices[6], "bid_6_sz": bid_sizes[6],
            "bid_7_px": bid_prices[7], "bid_7_sz": bid_sizes[7],
            "bid_8_px": bid_prices[8], "bid_8_sz": bid_sizes[8],
            "bid_9_px": bid_prices[9], "bid_9_sz": bid_sizes[9],
            "ask_0_px": ask_prices[0], "ask_0_sz": ask_sizes[0],
            "ask_1_px": ask_prices[1], "ask_1_sz": ask_sizes[1],
            "ask_2_px": ask_prices[2], "ask_2_sz": ask_sizes[2],
            "ask_3_px": ask_prices[3], "ask_3_sz": ask_sizes[3],
            "ask_4_px": ask_prices[4], "ask_4_sz": ask_sizes[4],
            "ask_5_px": ask_prices[5], "ask_5_sz": ask_sizes[5],
            "ask_6_px": ask_prices[6], "ask_6_sz": ask_sizes[6],
            "ask_7_px": ask_prices[7], "ask_7_sz": ask_sizes[7],
            "ask_8_px": ask_prices[8], "ask_8_sz": ask_sizes[8],
            "ask_9_px": ask_prices[9], "ask_9_sz": ask_sizes[9]
        }

        if LOG_PROCESSED_MESSAGES:
            logging.info(f"[PROCESSED ORDER BOOK] {processed_order_book}")

        return processed_order_book

    except Exception as e:
        logging.error(f"[ERROR] Failed to process order book data: {order_book} - {e}")
        return None