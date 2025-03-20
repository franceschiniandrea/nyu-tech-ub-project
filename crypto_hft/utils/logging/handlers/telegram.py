from crypto_hft.utils.config import Config
# from loguru import Message
import msgspec
import aiosonic # type: ignore
import asyncio
from loguru import logger

class TelegramLogger(): 
    def __init__(self, max_buffer: int):
        # get the secrets from the config object
        config = Config()
        self.telegram_key = config.telegram_api_key
        self.chat_id = config.telegram_chat_id

        self.max_buffer = max_buffer

        # define the queue, queue size and shutdown flag
        self._queue: asyncio.Queue = asyncio.Queue()
        self._log_message_buffer: list[str] = []
        self._current_buffer_size = 0
        self._shutdown_flag = False
        self._ev_loop = asyncio.get_event_loop()

        # define encoders and decoders
        self.json_encoder = msgspec.json.Encoder()
        self.json_decoder = msgspec.json.Decoder()

        # define url, headers, and http client to make requests to telegram
        self.url = f'https://api.telegram.org/bot{config.telegram_api_key}/sendMessage'
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.client = aiosonic.HTTPClient()

    async def start_log_ingestor(self): 
        queue = self._queue
        log_message_buffer = self._log_message_buffer
        while not self._shutdown_flag: 
            try:
                logger.trace('awaiting to get log from queue')
                log = await queue.get()
                logger.trace('received log from queue')
                level = log['record']['level']['no']

                log_message_buffer.append(log['text'])
                self._current_buffer_size += 1

                if level >= 40: 
                    logger.trace('flushing buffer due to error')
                    await self._flush_buffer()
                else: 
                    is_buffer_full = self._current_buffer_size >= self.max_buffer
                    is_buffer_old = False # todo add logic here

                    # flush buffer if old or full
                    if is_buffer_full or is_buffer_old: 
                        logger.trace('buffer full, size=', self._current_buffer_size)
                        await self._flush_buffer()

                queue.task_done()
            except Exception as e: 
                raise Exception('log writer loop:', e)
            
    async def _flush_buffer(self) -> None:
        tasks: list[asyncio.Task] = []
        logger.trace('flushing buffer, buffer=')

        try:
            for log in self._log_message_buffer: 
                payload = {
                    'chat_id': self.chat_id,
                    'text': log,
                    'disable_web_page_preview': True
                }
                tasks.append(
                    self.client.post(
                        url=self.url,
                        headers=self.headers,
                        data=self.json_encoder.encode(payload)
                    )
                )

            logger.trace('gathering all the tasks', tasks)
            res = await asyncio.gather(*tasks)
            logger.trace('done', res)
            self._log_message_buffer.clear()
            self._current_buffer_size = 0
        except Exception as err: 
            print(f'Failed to send message to telegram', err)

    def submit_log(self, message: str): 
        logger.trace('processing message' , message)
        data = self.json_decoder.decode(str(message))
        
        self._queue.put_nowait(data)

    async def shutdown(self):
        logger.trace('setting shutdown flag to true')
        self._shutdown_flag = True
        await self._queue.join()
        if self._log_message_buffer: 
            await self._flush_buffer()

        await self.client.connector.cleanup()
        del self.client

    def stop(self): 
        logger.trace('shutting down')
        # asyncio.run(self.shutdown())
        self._ev_loop.run_until_complete(self.shutdown())