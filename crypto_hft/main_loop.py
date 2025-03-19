import asyncio
import logging
from crypto_hft.data_layer.websocket import WebSocketConsumer
from crypto_hft.data_layer.db_writer import MySQLDatabase, batch_insert_order_books
from crypto_hft.utils.config import Config
from datetime import datetime
import ciso8601


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)

config = Config()
shutdown_event = asyncio.Event()  

async def main():
    """
    Runs WebSocket consumer and MySQL database batch inserts concurrently.
    Handles graceful shutdown on interruption.
    """
    logging.info("[+] Starting WebSocket consumer and database writers...")

    # ✅ Initialize MySQL connection
    db = MySQLDatabase()
    await db.connect()

    # ✅ Initialize WebSocket Consumer (Make sure this is correct!)
    websocket = WebSocketConsumer()

    # ✅ Run WebSocket and MySQL processing in parallel
    websocket_task = asyncio.create_task(websocket.run())  # ✅ Starts WebSocket correctly
    db_task = asyncio.create_task(batch_insert_order_books(db))  # ✅ Runs async DB processing

    try:
        await asyncio.gather(websocket_task, db_task)  # ✅ Run both concurrently
    except asyncio.CancelledError:
        logging.warning("[!] Tasks cancelled. Shutting down gracefully...")
    finally:
        logging.info("[!] Closing WebSocket connection...")
        await websocket.shutdown()  # ✅ Ensures WebSocket closes cleanly

        logging.info("[!] Closing database connection...")
        await db.close()  # ✅ Properly close DB connection

        logging.info("[✅] Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("[!] KeyboardInterrupt received. Shutting down...")
        shutdown_event.set()  # ✅ Notify tasks to shut down
