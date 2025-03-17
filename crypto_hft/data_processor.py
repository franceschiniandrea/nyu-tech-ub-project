import logging
import time 


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
            "amount": trade.get("amount", trade.get("volume", 0)),  # ✅ Ensure amount is used, not volume
            "side": trade.get("side", "buy"),  # ✅ Default to "buy" if missing
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
        bid_prices = [bid["price"] for bid in bids] + [None] * (10 - len(bids))
        bid_sizes = [bid["amount"] for bid in bids] + [None] * (10 - len(bids))
        ask_prices = [ask["price"] for ask in asks] + [None] * (10 - len(asks))
        ask_sizes = [ask["amount"] for ask in asks] + [None] * (10 - len(asks))
        
        processed_order_book = {
            "exchange": order_book.get("exchange"),
            "symbol": order_book.get("symbol"),
            "timestamp": order_book.get("timestamp"),
            "local_timestamp": order_book.get("localTimestamp"),
            "bid0": bid_prices[0], "bid0_size": bid_sizes[0],
            "bid1": bid_prices[1], "bid1_size": bid_sizes[1],
            "bid2": bid_prices[2], "bid2_size": bid_sizes[2],
            "bid3": bid_prices[3], "bid3_size": bid_sizes[3],
            "bid4": bid_prices[4], "bid4_size": bid_sizes[4],
            "bid5": bid_prices[5], "bid5_size": bid_sizes[5],
            "bid6": bid_prices[6], "bid6_size": bid_sizes[6],
            "bid7": bid_prices[7], "bid7_size": bid_sizes[7],
            "bid8": bid_prices[8], "bid8_size": bid_sizes[8],
            "bid9": bid_prices[9], "bid9_size": bid_sizes[9],
            "ask0": ask_prices[0], "ask0_size": ask_sizes[0],
            "ask1": ask_prices[1], "ask1_size": ask_sizes[1],
            "ask2": ask_prices[2], "ask2_size": ask_sizes[2],
            "ask3": ask_prices[3], "ask3_size": ask_sizes[3],
            "ask4": ask_prices[4], "ask4_size": ask_sizes[4],
            "ask5": ask_prices[5], "ask5_size": ask_sizes[5],
            "ask6": ask_prices[6], "ask6_size": ask_sizes[6],
            "ask7": ask_prices[7], "ask7_size": ask_sizes[7],
            "ask8": ask_prices[8], "ask8_size": ask_sizes[8],
            "ask9": ask_prices[9], "ask9_size": ask_sizes[9]
        }

        if LOG_PROCESSED_MESSAGES:
            logging.info(f"[PROCESSED ORDER BOOK] {processed_order_book}")

        return processed_order_book

    except Exception as e:
        logging.error(f"[ERROR] Failed to process order book data: {order_book} - {e}")
        return None