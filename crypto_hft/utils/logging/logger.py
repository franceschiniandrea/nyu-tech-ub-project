from loguru import logger
import sys
from crypto_hft.utils.logging.handlers import TelegramLogger
import asyncio 
def setup_logging(): 
    telegram_handler = TelegramLogger(max_buffer=10)
    event_loop = asyncio.get_event_loop()
    task = event_loop.create_task(telegram_handler.start_log_ingestor())

    # remove all existing handlers
    logger.remove()
    logger.add(
        sys.stderr, 
        format='<green>{time:YYYY-MM-DD HH:mm:ss!UTC} [UTC]</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>',
        colorize=True,
        level='DEBUG'
    )

    # only send to telegram warning and above
    logger.add(telegram_handler.submit_log, serialize=True, level='WARNING')
    return task

if __name__ == '__main__':
    async def main():
        task = setup_logging()

        for i in range(15): 
            logger.warning('test debug')

        logger.error('test error')
        await task
    
    asyncio.run(main())