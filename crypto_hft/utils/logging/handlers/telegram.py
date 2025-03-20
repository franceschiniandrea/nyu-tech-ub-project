from crypto_hft.utils.config import Config
# from loguru import Message
import msgspec
import aiosonic # type: ignore
import asyncio
from loguru import logger

class TelegramLogger(): 
    """Class for the Telegram Logger

    The `__init__` method takes care of starting the asynchronous task that
    checks the buffer size and flushes the messages to discord when the buffer size
    is > than `max_buffer`. If a log with level above 40 is sent (`CRITICAL` or `ERROR`)
    the buffer if flushed immediately.

    The logs are put in a `asyncio.Queue` synchronously, and processed asynchronously in the 
    `log_ingestor` task. When the buffer reaches the max size or is old enough, the function
    makes a batch request to the Telegram API and flushes the messages. 
    """
    def __init__(self, telegram_api_key: str, chat_id: str, max_buffer: int):
        """Initialize the TelegramLogger. 

        Parameters
        ----------
        telegram_api_key: str
            The Telegram Bot key, needed to authenticate to the API
        chat_id: str
            The Chat ID where to send the notifications
        max_buffer : int
            Maximum amount of messages to store before flushing the buffer and send 
            the HTTP request to the Telegram API
        """
        # store the inputs in the class instance
        self.telegram_key = telegram_api_key
        self.chat_id = chat_id
        self.max_buffer = max_buffer

        # the shutdown flag tells the log ingestor to stop
        # todo this might not be needed
        self._shutdown_flag = False

        # the queue takes care of storing the incoming logs
        self._queue: asyncio.Queue = asyncio.Queue()

        # the buffer stores the messages until they are ready to be sent out
        #   we also store the current buffer size to avoid calling len() every time
        self._log_message_buffer: list[str] = []
        self._current_buffer_size = 0

        # pre-initialize the encoder and decoder to handle json efficiently 
        self.json_encoder = msgspec.json.Encoder()
        self.json_decoder = msgspec.json.Decoder()

        # define url, headers, and the http client to make requests to telegram
        self.url = f'https://api.telegram.org/bot{telegram_api_key}/sendMessage'
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.timeouts = Timeouts(request_timeout=10)
        self.client = aiosonic.HTTPClient()

        # get the event loop and schedule the log ingestor task immediately
        self._ev_loop = asyncio.get_event_loop()
        self._ev_loop.create_task(self.start_log_ingestor())

    async def log_ingestor_loop(self): 
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