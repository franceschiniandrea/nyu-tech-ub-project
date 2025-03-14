import asyncio
import logging
from websocket import websocket_consumer
from db_writer import batch_insert_order_books, batch_insert_trades
from config import EXCHANGES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)

async def start_websockets():
    """Start WebSocket connections for all exchanges."""
    tasks = []
    for exchange in EXCHANGES:
        logging.info(f"[+] Starting WebSocket for {exchange}")
        tasks.append(asyncio.create_task(websocket_consumer(exchange)))  # ✅ Corrected
    return tasks  

async def start_db_writer():
    """Start database writer tasks."""
    logging.info("[+] Starting database writer...")
    return [
        asyncio.create_task(batch_insert_order_books()),
        asyncio.create_task(batch_insert_trades())
    ]  # ✅ Returns tasks properly

async def main():
    logging.info("[+] Starting main process...")

    websocket_tasks = await start_websockets()
    db_tasks = await start_db_writer()

    logging.info("[DEBUG] All tasks created, running asyncio.gather()...")
    
    await asyncio.gather(*websocket_tasks, *db_tasks)  # This should not hang

if __name__ == "__main__":
    asyncio.run(main())



if __name__ == "__main__":
    asyncio.run(main())  # ✅ Ensures everything runs correctly
