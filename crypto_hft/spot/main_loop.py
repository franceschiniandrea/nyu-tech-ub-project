import asyncio
import logging
import os
import sys
import uvloop  

from crypto_hft.spot.queue_manager import order_book_queues, trade_queues
from crypto_hft.spot.websocket_streamer import WebsocketStreamer
from crypto_hft.spot.websocket import WebSocketConsumer
from crypto_hft.spot.db_writer import PostgreSQLDatabase, QueueProcessor
from crypto_hft.utils.config import Config

# -----------------------------
# 🔧 Setup Logging
# -----------------------------
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler("/home/ss16430/nyu-tech-ub-project/hft_error.log"),
        logging.StreamHandler()
    ]
)

# -----------------------------
# ✅ Main Application Logic
# -----------------------------
async def main():
    tasks = []
    logging.info("[+] Starting WebSocket consumer and database writers...")

    config = Config()
    websocket_streamer = WebsocketStreamer()
    websocket = WebSocketConsumer(websocket_streamer=websocket_streamer)
    db = PostgreSQLDatabase(config)

    try:
        await db.connect()
        queue_processor = QueueProcessor(db, config)
        logging.info("[✅] All components initialized.")

        tasks += [
            asyncio.create_task(websocket.run(), name='websocket_task'),
            asyncio.create_task(queue_processor.batch_insert_order_books(), name='ob_processor'),
            asyncio.create_task(queue_processor.batch_insert_trades(), name='trade_processor'),
            asyncio.create_task(websocket_streamer.start(), name='websocket_server'),
            # asyncio.create_task(monitor_queues(), name='queue_monitor'),
        ]

        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logging.warning("[!] KeyboardInterrupt received. Cancelling tasks...")
        for task in tasks:
            logging.info(f"Cancelling task: {task.get_name()}")
            task.cancel()
            await task

    except Exception as e:
        logging.error("[❌] Unhandled exception in main loop", exc_info=True)

    finally:
        await cleanup(websocket, queue_processor, db)


# -----------------------------
# 🧹 Graceful Shutdown
# -----------------------------
async def cleanup(websocket: WebSocketConsumer, queue_processor: QueueProcessor, db: PostgreSQLDatabase):
    logging.info("[🛑] Cleaning up all components...")
    try:
        await websocket.shutdown()
        await queue_processor.shutdown()
        await db.close()
    except Exception as e:
        logging.error("[⚠️] Error during cleanup", exc_info=True)
    logging.info("[✅] Shutdown complete.")

# -----------------------------
# 🎯 Entry Point
# -----------------------------
if __name__ == "__main__":
    try:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  
        asyncio.run(main())
    except Exception as e:
        logging.error("❌ Fatal error at top level", exc_info=True)
        sys.exit(1)