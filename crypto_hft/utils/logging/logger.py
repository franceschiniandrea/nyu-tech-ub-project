from loguru import logger
import sys
from crypto_hft.utils.logging.handlers import TelegramLogger
from crypto_hft.utils.config import Config
import asyncio 

def setup_logging(): 
    config = Config() 
    telegram_handler = TelegramLogger(telegram_api_key=config.telegram_api_key, chat_id=config.telegram_chat_id, max_buffer=10)

    # remove all existing handlers
    logger.remove()
    logger.add(
        sys.stderr, 
        format='<green>{time:YYYY-MM-DD HH:mm:ss!UTC} [UTC]</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>',
        colorize=True,
        level='DEBUG'
    )
    logger.add('test.log', level='TRACE', serialize=True, enqueue=True)

    # only send to telegram warning and above
    logger.add(telegram_handler.submit_log, serialize=True, level='WARNING')

if __name__ == '__main__':
    async def main():
        setup_logging()

        for i in range(15): 
            logger.warning('test debug')

        logger.error('test error')
    
    asyncio.run(main())