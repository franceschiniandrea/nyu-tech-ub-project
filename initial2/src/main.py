import asyncio
import logging
from websocket import websocket_consumer
from db_writer import batch_insert_order_books, batch_insert_trades
from config import EXCHANGES  # Ensure this contains a list of exchange names

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
    Runs WebSocket consumers for all exchanges and starts database batch insert tasks.
    """
    logging.info("[+] Starting WebSocket consumers and database writers...")

    # Start WebSocket consumers for each exchange
    websocket_tasks = [websocket_consumer(exchange) for exchange in EXCHANGES]

    # Start database batch insert tasks
    db_tasks = [batch_insert_order_books(), batch_insert_trades()]

    # Run all tasks concurrently
    try:
        await asyncio.gather(*websocket_tasks, *db_tasks)
    except asyncio.CancelledError:
        logging.warning("[!] Tasks cancelled, shutting down...")
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error in main: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("[!] KeyboardInterrupt received. Exiting...")
