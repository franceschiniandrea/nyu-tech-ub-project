# websocket_client.py
#version 1 #single websocket connection
# import asyncio
# import logging
# import json
# import aiohttp
# import urllib.parse
# from config import EXCHANGES, DATA_TYPES
# from symbol_mapper import EXCHANGE_SYMBOLS
# from data_processor import process_order_book_data, process_trade_data

# # Enable raw message logging
# LOG_RAW_MESSAGES = False # Set to False to disable raw message logging

# # Logging configuration
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("websocket_client.log"),
#         logging.StreamHandler()
#     ]
# )

# async def websocket_consumer():
#     """
#     Handles streaming from Tardis Machine WebSocket.
#     """
#     retries = 0

#     while retries < 5:  # Retry up to 5 times
#         try:
#             # Prepare stream options for WebSocket request
#             stream_options = [
#                 {
#                     "exchange": exchange,
#                     "symbols": EXCHANGE_SYMBOLS[exchange],
#                     "dataTypes": DATA_TYPES
#                 }
#                 for exchange in EXCHANGES
#             ]
            
#             options = urllib.parse.quote_plus(json.dumps(stream_options))
#             URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

#             async with aiohttp.ClientSession() as session:
#                 async with session.ws_connect(URL) as websocket:
#                     logging.info("[+] Connected to Tardis Machine WebSocket.")
#                     retries = 0  # Reset retry count on success

#                     # Process each incoming WebSocket message
#                     async for msg in websocket:
#                         if msg.type == aiohttp.WSMsgType.TEXT:
#                             raw_data = msg.data  # Raw JSON message

#                             if LOG_RAW_MESSAGES:
#                                 logging.info(f"Raw Message: {raw_data}")  # Log raw data

#                             data = json.loads(raw_data)  # Parse JSON message
#                             await update_data(data)  # Process data
                        
#                         elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
#                             logging.warning(f"[!] WebSocket closed. Reason: {msg.data}")
#                             break  # Exit loop to reconnect

#         except aiohttp.ClientConnectionError as e:
#             retries += 1
#             wait_time = 2 ** retries
#             logging.error(f"[WebSocket ERROR] Connection failed: {e} (Retry {retries}/5). Retrying in {wait_time}s...")
#             await asyncio.sleep(wait_time)

#     logging.critical("[WebSocket CRITICAL] Max retries reached. Exiting.")


# from symbol_mapper import REVERSE_SYMBOL_MAP  # Ensure we import the correct mappings
#version 2
import asyncio
import logging
import json
import aiohttp
import urllib.parse
from config import EXCHANGES, DATA_TYPES, BASE_TICKERS
from symbol_mapper import EXCHANGE_SYMBOLS, REVERSE_SYMBOL_MAP  
from data_processor import process_order_book_data, process_trade_data
from db_writer import order_book_queues, trade_queues  # Import queues

# Enable logging
LOG_RAW_MESSAGES = False  
LOG_PROCESSED_MESSAGES = True  

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("websocket_client.log"),
        logging.StreamHandler()
    ]
)
async def websocket_consumer(exchange):
    """
    Creates and maintains a WebSocket connection for a single exchange.
    Implements limited reconnection attempts.
    """
    logging.info(f"[+] Connecting to {exchange} WebSocket...")
    retries = 0  
    max_retries = 5  

    while retries < max_retries:
        try:
            # Prepare WebSocket request
            stream_options = {
                "exchange": exchange,
                "symbols": EXCHANGE_SYMBOLS[exchange],
                "dataTypes": DATA_TYPES
            }

            options = urllib.parse.quote_plus(json.dumps(stream_options))
            URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(URL) as websocket:
                    logging.info(f"[✅] Connected to {exchange} WebSocket.")
                    retries = 0  # ✅ Reset retries on successful connection

                    async for msg in websocket:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw_data = msg.data  
                            logging.info(f"[RAW] {exchange}: {raw_data[:100]}...")  # ✅ Log first 100 chars
                            data = json.loads(raw_data)  
                            await update_data(data, exchange)  

                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            logging.warning(f"[!] {exchange} WebSocket closed. Reason: {msg.data}")
                            break  # ✅ Exit loop, attempt reconnection

        except aiohttp.ClientConnectionError as e:
            retries += 1
            wait_time = min(2 ** retries, 60)  # ✅ Use exponential backoff but cap at 60s
            logging.error(f"[WebSocket ERROR] {exchange} connection failed: {e} (Retry {retries}/{max_retries}). Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)

    logging.critical(f"[WebSocket CRITICAL] {exchange} reached max retries ({max_retries}). Exiting process.")

async def update_data(trade, exchange):
    data_type = trade.get("type")
    received_symbol = trade.get("symbol")

    # Convert to the same format used as queue keys in db_writer
    queue_key = received_symbol.lower().replace("_", "")

    try:
        processed_data = None

        if data_type == "book_snapshot":
            processed_data = process_order_book_data(trade)
            if processed_data:
                await order_book_queues[queue_key].put(processed_data)
                logging.info(f"[QUEUED ORDER BOOK] {exchange} {received_symbol}: {processed_data}")

        elif data_type == "trade":
            processed_data = process_trade_data(trade)
            if processed_data:
                await trade_queues[queue_key].put(processed_data)
                logging.info(f"[QUEUED TRADE] {exchange} {received_symbol}: {processed_data}")

    except Exception as e:
        logging.error(f"Error processing data from {exchange} ({received_symbol}): {e}")

# async def main():
#     """
#     Runs multiple WebSocket connections in parallel.
#     """
#     tasks = [websocket_consumer(exchange) for exchange in EXCHANGES]
#     await asyncio.gather(*tasks)  

# if __name__ == "__main__":
#     asyncio.run(main())
    
