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
LOG_PROCESSED_MESSAGES = False

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
                "symbols": EXCHANGE_SYMBOLS.get(exchange, []),  # Avoid KeyError
                "dataTypes": DATA_TYPES
            }

            options = urllib.parse.quote_plus(json.dumps(stream_options))
            URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(URL) as websocket:
                    logging.info(f"[✅] Connected to {exchange} WebSocket.")
                    retries = 0  # ✅ Reset retries on successful connection
                    message_count = 0  # ✅ Track received messages

                    async for msg in websocket:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw_data = msg.data  
                            message_count += 1
                            logging.info(f"[RECEIVED] {exchange} | Message #{message_count}")  # ✅ Count messages
                            
                            if LOG_RAW_MESSAGES:
                                logging.debug(f"[RAW] {exchange}: {raw_data[:200]}...")  # ✅ Log first 200 chars

                            data = json.loads(raw_data)  
                            await update_data(data, exchange)  

                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            logging.warning(f"[!] {exchange} WebSocket closed unexpectedly. Reason: {msg.data}. Reconnecting in 5s...")
                            await asyncio.sleep(5)
                            break  # ✅ Allows reconnection loop to restart

        except aiohttp.ClientConnectionError as e:
            retries += 1
            wait_time = min(2 ** retries, 60)  # ✅ Use exponential backoff but cap at 60s
            logging.error(f"[WebSocket ERROR] {exchange} connection failed: {e} (Retry {retries}/{max_retries}). Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)

    logging.critical(f"[WebSocket CRITICAL] {exchange} reached max retries ({max_retries}). Exiting process.")

async def update_data(trade, exchange):
    """
    Processes raw data received from WebSocket and queues it for database insertion.
    Logs raw messages and processed data.
    """
    data_type = trade.get("type")
    received_symbol = trade.get("symbol", "").lower()  # Ensure lowercase for mapping

    # 🔹 Ensure correct symbol mapping
    if exchange == "hyperliquid":
        received_symbol = REVERSE_SYMBOL_MAP["hyperliquid"].get(received_symbol, received_symbol)  # Convert "btc" → "BTC_USDT"
    else:
        received_symbol = REVERSE_SYMBOL_MAP.get(exchange, {}).get(received_symbol, received_symbol)
        received_symbol = received_symbol.upper().replace("-", "_")  # Standardize for DB format

    # logging.info(f"[FULL RAW] {exchange} | {received_symbol} | {json.dumps(trade, indent=2)}")

    try:
        processed_data = None

        if LOG_RAW_MESSAGES:
            logging.info(f"[RAW] {exchange} | {received_symbol} | {json.dumps(trade)}")  

        if data_type == "book_snapshot":
            processed_data = process_order_book_data(trade)

            if processed_data:
                logging.info(f"[PROCESSED] Order Book | {exchange} | {received_symbol} | {json.dumps(processed_data)}")  

                if received_symbol in order_book_queues:
                    queue_size_before = order_book_queues[received_symbol].qsize()
                    await order_book_queues[received_symbol].put(processed_data)
                    queue_size_after = order_book_queues[received_symbol].qsize()
                    logging.info(f"[QUEUED] Order Book | {exchange} | {received_symbol} | Before: {queue_size_before} | After: {queue_size_after}")
                else:
                    logging.error(f"[ERROR] Symbol {received_symbol} NOT found in order_book_queues! Expected: {list(order_book_queues.keys())}")

        elif data_type == "trade":
            #logging.info(f"[DEBUG] Raw Trade Data: {json.dumps(trade, indent=2)}")

            processed_data = process_trade_data(trade)

            if processed_data:
                logging.info(f"[PROCESSED] Trade | {exchange} | {received_symbol} | {json.dumps(processed_data)[:500]}")  

                if received_symbol in trade_queues:
                    queue_size_before = trade_queues[received_symbol].qsize()
                    await trade_queues[received_symbol].put(processed_data)
                    queue_size_after = trade_queues[received_symbol].qsize()
                    logging.info(f"[QUEUED] Trade | {exchange} | {received_symbol} | Before: {queue_size_before} | After: {queue_size_after}")
                else:
                    logging.error(f"[ERROR] Symbol {received_symbol} NOT found in trade_queues! Expected: {list(trade_queues.keys())}")

    except Exception as e:
        logging.error(f"[ERROR] Failed to queue data for {exchange} ({received_symbol}): {e}")
 

# async def main():
#     """
#     Runs WebSocket consumers for all exchanges concurrently.
#     """
#     logging.info("[+] Starting WebSocket consumers...")

#     # Start WebSocket consumers for each exchange
#     websocket_tasks = [websocket_consumer(exchange) for exchange in EXCHANGES]

#     try:
#         await asyncio.gather(*websocket_tasks)
#     except asyncio.CancelledError:
#         logging.warning("[!] Tasks cancelled, shutting down...")
#     except Exception as e:
#         logging.error(f"[ERROR] Unexpected error in main: {e}")

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logging.info("[!] KeyboardInterrupt received. Exiting...")
