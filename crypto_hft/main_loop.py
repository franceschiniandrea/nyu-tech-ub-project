
import asyncio
from crypto_hft.data_layer.websocket import WebSocketConsumer
from crypto_hft.data_layer.db_writer import MySQLDatabase, QueueProcessor
from crypto_hft.utils.config import Config
from crypto_hft.utils.logging import setup_logging
from loguru import logger

async def main():
    """
    Runs WebSocket consumer and MySQL database batch inserts concurrently for both order books and trades.
    Handles graceful shutdown on interruption.
    """
    tasks = []
    setup_logging()
    logger.info("[+] Starting WebSocket consumer and database writers...")

    config = Config()
    
    # Initialize WebSocket Consumer
    websocket = WebSocketConsumer()

    # Initialize Database & Queue Processor
    db = MySQLDatabase(config)
    await db.connect()
    queue_processor = QueueProcessor(db, config)
    logger.info('all things initialized, now starting the try/except loop')

    try:
        # Run WebSocket and MySQL processing in parallel
        loop = asyncio.get_event_loop() 
        tasks += [
            loop.create_task(websocket.run(), name='websocket_task'),
            loop.create_task(queue_processor.batch_insert_order_books(),name='ob_processor'),
            loop.create_task(queue_processor.batch_insert_trades(),name='trade_processor'),
        ]
        logger.info('starting main loop')
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.error('Handling CancelledError in main_loop. Cancelling tasks...')
        for task in tasks: 
            logger.info('cancelling task', task, task.get_name(), task._state)
            task.cancel()
            await task
    except Exception as e:
        logger.error(f"[❌] Unexpected error: {e}")
    finally:
        logger.debug('reached end of the statement, doing final cleanup')
        await cleanup(websocket, queue_processor, db)

async def cleanup(websocket: WebSocketConsumer, queue_processor: QueueProcessor, db: MySQLDatabase):
    """Handles clean shutdown of WebSocket, queue processor, and database connections."""
    logger.info("[!] Closing WebSocket connection...")
    await websocket.shutdown()

    logger.info("[!] Stopping queue processor...")
    await queue_processor.shutdown()

    logger.info("[!] Closing database connection...")
    await db.close()

    logger.info("[✅] Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())