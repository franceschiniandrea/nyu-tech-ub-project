
import asyncio
import logging
from crypto_hft.data_layer.websocket import WebSocketConsumer
from crypto_hft.data_layer.db_writer import MySQLDatabase, QueueProcessor
from crypto_hft.utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)

async def main():
    """
    Runs WebSocket consumer and MySQL database batch inserts concurrently for both order books and trades.
    Handles graceful shutdown on interruption.
    """
    logging.info("[+] Starting WebSocket consumer and database writers...")

    config = Config()
    
    # Initialize WebSocket Consumer
    websocket = WebSocketConsumer()

    # Initialize Database & Queue Processor
    db = MySQLDatabase(config)
    await db.connect()
    queue_processor = QueueProcessor(db, config)

    # Run WebSocket and MySQL processing in parallel
    tasks = [
        asyncio.create_task(websocket.run()),
        asyncio.create_task(queue_processor.batch_insert_order_books()),
        asyncio.create_task(queue_processor.batch_insert_trades()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.warning("[!] Tasks cancelled. Shutting down gracefully...")
    except Exception as e:
        logging.error(f"[❌] Unexpected error: {e}")
    finally:
        await cleanup(websocket, queue_processor, db)

async def cleanup(websocket: WebSocketConsumer, queue_processor: QueueProcessor, db: MySQLDatabase):
    """Handles clean shutdown of WebSocket, queue processor, and database connections."""
    logging.info("[!] Closing WebSocket connection...")
    await websocket.shutdown()

    logging.info("[!] Stopping queue processor...")
    await queue_processor.shutdown()

    logging.info("[!] Closing database connection...")
    await db.close()

    logging.info("[✅] Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("[!] KeyboardInterrupt received. Shutting down...")
