import asyncio
import logging
import signal
from crypto_hft.utils.config import Config

from create_queue import order_book_queues_perps, trade_queues_perps
from streamer import main as run_streamer
from postgres_utils import PostgreSQLDatabase, QueueProcessor

# --- Global state ---
shutdown_event = asyncio.Event()
running_tasks: list[asyncio.Task] = []


# --- Signal handling ---
def handle_signal():
    logging.warning("🛑 Received shutdown signal. Cancelling tasks...")
    shutdown_event.set()
    for task in running_tasks:
        task.cancel()

def setup_signal_handlers():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)


# --- Main ---
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    logging.info("[+] Starting Crypto Collector System")
    setup_signal_handlers()

    config = Config()
    db = PostgreSQLDatabase(config)
    await db.connect()

    processor = QueueProcessor(db, config)

    # Start background tasks
    ob_task = asyncio.create_task(processor.batch_insert_order_books(order_book_queues_perps))
    tr_task = asyncio.create_task(processor.batch_insert_trades(trade_queues_perps))
    streamer_task = asyncio.create_task(run_streamer())

    running_tasks.extend([ob_task, tr_task, streamer_task])

    try:
        await shutdown_event.wait()
    finally:
        logging.info("📦 Shutting down components...")
        await processor.shutdown()
        await db.close()
        await asyncio.gather(*running_tasks, return_exceptions=True)
        logging.info("✅ All components shutdown complete.")


# --- Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("🛑 Interrupted manually")
