import asyncio
import logging
import aiohttp
import urllib.parse
import msgspec
from crypto_hft.utils.config import Config
from crypto_hft.utils.symbol_mapper import EXCHANGE_SYMBOLS, REVERSE_SYMBOL_MAP
from crypto_hft.data_layer.data_processor import process_order_book_data, process_trade_data
from crypto_hft.data_layer.queue_manager import order_book_queues, trade_queues
from crypto_hft.utils.logging import setup_logging
from loguru import logger

class WebSocketConsumer:
    def __init__(self) -> None:
        self.config = Config()
        self.json_encoder = msgspec.json.Encoder()
        self.json_decoder = msgspec.json.Decoder()
        self.shutdown_event = asyncio.Event()
        self.message_counter: int = 0  
        self.ws_url: str = self.build_ws_url()
        self.orderbook_counter = 0
        self.trade_counter = 0
        self.first_raw_logged = False
    
    def build_ws_url(self) -> str:
        """Constructs the WebSocket URL with properly formatted symbols."""
        options_data : list = [
            {"exchange": ex,
            "symbols": EXCHANGE_SYMBOLS.get(ex, []),
            "dataTypes": self.config.data_types}
            for ex in self.config.exchanges
        ]
        options = urllib.parse.quote_plus(self.json_encoder.encode(options_data).decode())

        ws_url = f"ws://localhost:8001/ws-stream-normalized?options={options}"
        return ws_url

    async def connect(self) -> None:
        '''establishes connection to get data'''
        '''exceptions: connection issues like network timouts, websocket server unreachable'''
        retries :int = 0
        max_retries_per_minute :int = self.config.max_retries  # Retrieve max retries from config
        retry_wait_time :int = self.config.retry_wait_time  # Retrieve retry wait time from config

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
                    logging.error(" Max retries per minute reached. Waiting before retrying...")
                    await asyncio.sleep(retry_wait_time)  # Wait before retrying
                else:
                    # Exponential backoff
                    backoff_time = min(2 ** retries, 60)  # Exponentially increase the wait time (up to a max of 60 seconds)
                    logging.info(f" Retrying after {backoff_time} seconds...")
                    await asyncio.sleep(backoff_time)  # Sleep before retrying

                await self.reconnect()  
                continue


    async def handle_message(self, msg: aiohttp.WSMessage) -> None:
        """Handles incoming WebSocket messages and processes them accordingly."""
        try:
            if msg.type == aiohttp.WSMsgType.TEXT:
                # Process the message when it's of type TEXT
                data :dict = self.json_decoder.decode(msg.data)
                if not self.first_raw_logged:
                    logging.info(f"[FIRST RAW MESSAGE] {data}")
                    self.first_raw_logged = True  
                await self.update_data(data, data["exchange"])

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                # Handle WebSocket closure (optional)
                logging.error(f" WebSocket closed. Reason: {msg.data}")
                await self.reconnect() 

            elif msg.type == aiohttp.WSMsgType.ERROR:
                # Handle WebSocket error
                logging.error(f" WebSocket encountered an error: {msg.data}")
                await self.reconnect()

        except Exception as e:
            logging.error(f"[ Error processing message: {e}")
    
    async def reconnect(self)-> None:
        retries = 0
        max_retries = self.config.max_retries
        while retries < max_retries:
            try:
                logging.info(f"[🔄] Attempting to reconnect... (Attempt {retries + 1}/{max_retries})")
                await self.connect()  # Reconnect
                break  # Successfully reconnected, exit loop
            except Exception as e:
                retries += 1
                logging.error(f"[❌] Reconnection failed: {e}. Retrying...")
                await asyncio.sleep(2 ** retries)  # Exponential backoff: wait longer after each failure


    async def update_data(self, data: dict, exchange: str) -> None:
        """Processes WebSocket messages and logs processed & queued data."""
        try: 
            data_type = data['type']
            received_symbol = data['symbol']
        except KeyError as err: 
            logger.error('Key not found while running update_data in websocket, err: %s' % err)
            
        standardized_symbol :str = REVERSE_SYMBOL_MAP[exchange].get(received_symbol, received_symbol)
        processed_data = None

        if data_type == "book_snapshot":
            processed_data = process_order_book_data(data, standardized_symbol)
        elif data_type == "trade":
            processed_data = process_trade_data(data, standardized_symbol)

        if processed_data:
            # Log only the first and every 5000th queued order book/trade message
            
            #logging.info(f"[PROCESSED MESSAGE] {data_type.upper()} {standardized_symbol}: {processed_data}")

            # Determine the correct queue and enqueue data
            queue = order_book_queues.get(standardized_symbol) if data_type == "book_snapshot" else trade_queues.get(standardized_symbol)

            if queue:
                await queue.put(processed_data)
                if (data_type == "book_snapshot" and (self.orderbook_counter == 1 or self.orderbook_counter % 5000 == 0)) or \
                (data_type == "trade" and (self.trade_counter == 1 or self.trade_counter % 5000 == 0)):
                    logging.info(f"[QUEUED {data_type.upper()} MESSAGE {self.orderbook_counter if data_type == 'book_snapshot' else self.trade_counter}] {standardized_symbol}: {processed_data}")
                # logging.info(f"[QUEUED MESSAGE] {data_type.upper()} {standardized_symbol}: {processed_data}")
            else:
                logging.warning(f"[WARNING] No queue found for {standardized_symbol}")

    async def run(self) -> None:
        await self.connect()

    async def shutdown(self)-> None:
        logging.info("[!] Shutting down WebSocket Consumer...")
        self.shutdown_event.set()

if __name__ == "__main__":
    setup_logging()
    consumer = WebSocketConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        asyncio.run(consumer.shutdown())
