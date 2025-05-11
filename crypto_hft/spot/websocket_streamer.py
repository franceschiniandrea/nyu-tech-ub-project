import websockets
from websockets import broadcast
import asyncio
from loguru import logger
import msgspec
import numpy as np

class WebsocketStreamer(): 
    def __init__(self) -> None: 
        # connections is a dict mapping the subscribed tokens to the websocket
        # server connections
        self.connections: dict[str, set[websockets.ServerConnection]] = {
            'all': set(),
        }
        self.json_encoder = msgspec.json.Encoder()

    def send_update(self, token: str, data: dict) -> None: 
        all_users = self.connections['all']
        specific_users = self.connections.get(token, set())
        users = all_users.union(specific_users)
        
        if len(users) > 0: 
            # logger.info(f"Sending update to {len(users)} users for token {token}")
            broadcast(users, self.json_encoder.encode(data))

    async def handler(self, ws: websockets.ServerConnection) -> None: 
        # logger.info(path)
        req = ws.request
        subscribed_path = None
        if req is None: 
            logger.info('WS Connection has no path, subscribing to all updates')
            self.connections['all'].add(ws)
            subscribed_path = 'all'
        else: 
            path = req.path.replace('/', '')
            logger.info(f"Subscribing user to {path}")
            if path not in self.connections: 
                self.connections[path] = set()
            self.connections[path].add(ws)
            subscribed_path = path
        
        try: 
            async for msg in ws: 
                logger.info(f'Received message: {str(msg)}')
        except Exception as e: 
            logger.error(f"Error in WebSocket handler: {e}")
        finally: 
            logger.info(f"Unsubscribing user from {path}")
            self.connections[subscribed_path].remove(ws)
            
    async def monitor_connections(self):
        while True: 
            await asyncio.sleep(5)
            if len(self.connections) == 0:
                logger.info("No active connections")
            for token, users in self.connections.items(): 
                logger.info(f"Token: {token}, Users: {len(users)}")

    async def serve_websocket(self): 
        logger.info("Starting WebSocket server...")
        async with websockets.serve(self.handler, 'localhost', 9999) as server: 
            await server.serve_forever()

    async def test_ping(self): 
        tokens = ['ada_usdt', 'btc_usdt']
        while True: 
            await asyncio.sleep(5)

            token = np.random.choice(tokens)

            self.send_update(token, {
                'type': 'order_book',
                'bids': [[1, 2], [3, 4]],
            })

    async def start(self): 
        logger.info("Starting main process")

        try: 
            tasks = [
                asyncio.create_task(self.serve_websocket(), name='websocket_server'),
                # asyncio.create_task(self.test_ping(), name='ping_test'),
                asyncio.create_task(self.monitor_connections(), name='monitor_connections'),
            ]

            await asyncio.gather(*tasks)
        except asyncio.CancelledError: 
            logger.info('KeyboardInterrupt received. Cancelling tasks...')
            for task in tasks: 
                logger.info(f"Cancelling task: {task.get_name()}")
                task.cancel()
                await task
    
if __name__ == "__main__": 
    streamer = WebsocketStreamer()
    asyncio.run(streamer.start())