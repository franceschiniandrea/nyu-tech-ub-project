import asyncio
import logging
import json
import aiohttp
import urllib.parse
from crypto_hft.utils.config import Config
from crypto_hft.utils.symbol_mapper import EXCHANGE_SYMBOLS, REVERSE_SYMBOL_MAP
from crypto_hft.data_layer.data_processor import process_order_book_data, process_trade_data
from crypto_hft.data_layer.db_writer import order_book_queues, trade_queues

# config = Config()

# async def websocket_consumer(exchange):
#     """Maintains a WebSocket connection for an exchange with reconnection."""
#     logging.info(f"[+] Connecting to {exchange} WebSocket...")
#     # ✅ DEBUG: Print the symbols being sent to Coinbase
#     #if exchange == "coinbase":
#         #logging.info(f"[DEBUG] Coinbase Symbols Sent: {EXCHANGE_SYMBOLS.get(exchange, [])}")
#     retries = 0  
#     max_retries = 20  

#     while True:
#         try:
#             stream_options = {
#                 "exchange": exchange,
#                 "symbols": EXCHANGE_SYMBOLS.get(exchange, []),
#                 "dataTypes": config.data_types
#             }

#             options = urllib.parse.quote_plus(json.dumps(stream_options))
#             URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

#             async with aiohttp.ClientSession() as session:
#                 async with session.ws_connect(URL) as websocket:
#                     logging.info(f"[✅] Connected to {exchange} WebSocket.")
#                     retries = 0  

#                     message_count = 0  

#                     async for msg in websocket:
#                         if msg.type == aiohttp.WSMsgType.TEXT:
#                             raw_data = msg.data  
#                             message_count += 1

#                             if message_count % 500 == 0:  # ✅ Log every 500 messages only
#                                 logging.info(f"[RECEIVED] {exchange} | Message #{message_count}")

#                             data = json.loads(raw_data)  
#                             #if exchange == "coinbase":
#                                 #logging.info(f"[DEBUG] Coinbase Raw Message: {raw_data}")
#                             await update_data(data, exchange)  

#                         elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
#                             logging.warning(f"[!] {exchange} WebSocket closed unexpectedly. Reconnecting in 5s...")
#                             await asyncio.sleep(5)
#                             break  
                        
#                         else: 
#                             raise ValueError('unhandled message type %s' % msg.type)

#         except aiohttp.ClientConnectionError as e:
#             retries += 1
#             wait_time = min(2 ** retries, 60)  
#             logging.error(f"[WebSocket ERROR] {exchange} connection failed: {e} (Retry {retries}/{max_retries}). Retrying in {wait_time}s...")
#             await asyncio.sleep(wait_time)

# async def update_data(trade, exchange):
#     """Processes WebSocket messages and queues them for database insertion."""
#     data_type = trade.get("type")
#     received_symbol = trade.get("symbol", "").lower()

#     if exchange == "hyperliquid":
#         received_symbol = REVERSE_SYMBOL_MAP["hyperliquid"].get(received_symbol, received_symbol)
#     else:
#         received_symbol = REVERSE_SYMBOL_MAP.get(exchange, {}).get(received_symbol, received_symbol)
#         received_symbol = received_symbol.upper().replace("-", "_")

#     try:
#         processed_data = None

#         if data_type == "book_snapshot":
#             processed_data = process_order_book_data(trade)

#             if processed_data and received_symbol in order_book_queues:
#                 await order_book_queues[received_symbol].put(processed_data)
#                 queue_size = order_book_queues[received_symbol].qsize()

#                 if queue_size % 500 == 0:  # ✅ Log only every 500th insert
#                     logging.info(f"[QUEUED] Order Book | {exchange} | {received_symbol} | Queue Size: {queue_size}")

#         elif data_type == "trade":
#             processed_data = process_trade_data(trade)

#             if processed_data and received_symbol in trade_queues:
#                 await trade_queues[received_symbol].put(processed_data)
#                 queue_size = trade_queues[received_symbol].qsize()

#                 if queue_size % 500 == 0:  # ✅ Log only every 500th trade insert
#                     logging.info(f"[QUEUED] Trade | {exchange} | {received_symbol} | Queue Size: {queue_size}")

#     except Exception as e:
#         logging.error(f"[ERROR] Failed to queue data for {exchange} ({received_symbol}): {e}")

# async def main():
#     """Runs WebSocket consumers for all exchanges concurrently."""
#     logging.info("[+] Starting WebSocket consumers...")
#     await asyncio.gather(*[websocket_consumer(exchange) for exchange in config.exchanges])

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logging.info("[!] KeyboardInterrupt received. Exiting...")

import asyncio
import logging
import aiohttp
import urllib.parse
import msgspec
import random

from crypto_hft.utils.config import Config
from crypto_hft.utils.symbol_mapper import EXCHANGE_SYMBOLS, REVERSE_SYMBOL_MAP
from crypto_hft.data_layer.data_processor import process_order_book_data, process_trade_data
from crypto_hft.data_layer.queue_manager import order_book_queues, trade_queues

class WebSocketConsumer:
    def __init__(self):
        self.config = Config()
        self.json_encoder = msgspec.json.Encoder()
        self.json_decoder = msgspec.json.Decoder()
        self.shutdown_event = asyncio.Event()
        self.message_counter = 0  
        self.ws_url = self.build_ws_url()
    
        

    def build_ws_url(self):
        """Constructs the WebSocket URL with properly formatted symbols."""
        options_data = [
            {"exchange": ex,
            "symbols": EXCHANGE_SYMBOLS.get(ex, []),
            "dataTypes": self.config.data_types}
            for ex in self.config.exchanges
        ]
        options = urllib.parse.quote_plus(self.json_encoder.encode(options_data).decode())

        ws_url = f"ws://localhost:8001/ws-stream-normalized?options={options}"
        return ws_url


    async def connect(self):
        retries = 0
        max_retries_per_minute = 5  

        while not self.shutdown_event.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url) as websocket:
                        logging.info("[✅] Connected to WebSocket.")
                        retries = 0  # Reset retries after successful connection
                    
                        async for msg in websocket:
                            if self.shutdown_event.is_set():
                                return
                            await self.handle_message(msg)

            except aiohttp.ClientConnectionError as e:
                retries += 1
                logging.warning(f"[!] WebSocket disconnected: {e}. Attempting to reconnect... (Retry {retries}/{max_retries_per_minute})")
            
                if retries >= max_retries_per_minute:
                    logging.error("[❌] Max retries per minute reached. Waiting before retrying...")
                    await asyncio.sleep(10)  # Wait for 10 seconds if too many failures
            
            
                continue  


    async def handle_message(self, msg):
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = self.json_decoder.decode(msg.data)
            # logging.info(f"[RAW MESSAGE] {data}")
            await self.update_data(data, data["exchange"])

    async def update_data(self, data, exchange):
        """Processes WebSocket messages and logs processed & queued data."""
        data_type = data.get("type")

        # Get received symbol from WebSocket message
        received_symbol = data.get("symbol")

        standardized_symbol = REVERSE_SYMBOL_MAP[exchange].get(received_symbol, received_symbol)
        processed_data = None

        if data_type == "book_snapshot":
            processed_data = process_order_book_data(data, standardized_symbol)
        elif data_type == "trade":
            processed_data = process_trade_data(data, standardized_symbol)

        if processed_data:
            #logging.info(f"[PROCESSED MESSAGE] {data_type.upper()} {standardized_symbol}: {processed_data}")

            # Determine the correct queue and enqueue data
            queue = order_book_queues.get(standardized_symbol) if data_type == "book_snapshot" else trade_queues.get(standardized_symbol)

            if queue:
                await queue.put(processed_data)
                #logging.info(f"[QUEUED MESSAGE] {data_type.upper()} {standardized_symbol}: {processed_data}")
            else:
                logging.warning(f"[WARNING] No queue found for {standardized_symbol}")

    async def run(self):
        await self.connect()

    async def shutdown(self):
        logging.info("[!] Shutting down WebSocket Consumer...")
        self.shutdown_event.set()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = WebSocketConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        asyncio.run(consumer.shutdown())
