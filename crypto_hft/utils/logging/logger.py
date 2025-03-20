from loguru import logger
import sys
from crypto_hft.utils.logging.handlers import TelegramLogger
from crypto_hft.utils.config import Config
import asyncio 

def setup_logging(): 
    """Setup Loguru Loggers

    Adds the following handlers to `loguru`: 
    * Telegram: send messages with level `WARNING` and above
    * File: store all logs (`TRACE` and above) in a file, serialized
    * Console: display logs with level `DEBUG` and above in the console
    """
    config = Config() 
    telegram_handler = TelegramLogger(
        telegram_api_key=config.telegram_api_key, 
        chat_id=config.telegram_chat_id, 
        max_buffer=config.logger_telegram_max_buffer
    )

    # remove all existing handlers
    logger.remove()

    # standard console logger
    logger.add(
        sys.stderr, 
        format='<green>{time:YYYY-MM-DD HH:mm:ss!UTC} [UTC]</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>',
        colorize=True,
        level=config.logger_console_min_level
    )

    # logger dumping in .log file
    logger.add(
        sink=config.logger_file_filepath, 
        level=config.logger_file_min_level, 
        serialize=True, 
        enqueue=True
    )

    # logger sending messages to telegram
    logger.add(
        sink=telegram_handler.submit_log, 
        serialize=True, 
        level=config.logger_telegram_min_level
    )

if __name__ == '__main__':
    async def main():
        setup_logging()

        for i in range(15): 
            logger.warning('test debug')

        logger.error('test error')
    
    asyncio.run(main())