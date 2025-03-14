#https://medium.com/tardis-dev/live-liquidations-monitor-for-top-cryptocurrency-exchanges-ac8e429e9556
import asyncio
import aiohttp
import json
import urllib.parse

async def run():
    """WebSocket client that connects and prints received data."""
    data_types = ["book_snapshot_10_100ms"] 

    stream_options = [
        {
            "exchange": "coinbase",
            "symbols": ["BTC-USD"], # Coinbase uses hyphen in symbol
            "dataTypes": data_types
        }
    ]

    options = urllib.parse.quote_plus(json.dumps(stream_options))
    URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"  

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(URL) as websocket:
            print("[+] Connected to WebSocket. Receiving data...\n")

            async for msg in websocket:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    print(msg.data)  # Print received message
                elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    print("[!] WebSocket closed. Reason:", msg.data)
                    break  # Exit on error or closure

asyncio.run(run())


#Example output:
#{"type":"book_snapshot","symbol":"BTC-USD","exchange":"coinbase",
# "name":"book_snapshot_10_100ms",
# "depth":10,
# "interval":100,
# "bids":[{"price":79983.29,"amount":0.34433683},{"price":79983.23,"amount":0.01875393},{"price":79983.22,"amount":0.03125655},{"price":79973.79,"amount":0.001},{"price":79970.35,"amount":0.00012787},{"price":79970.34,"amount":0.00104714},{"price":79970.33,"amount":0.00378626},{"price":79970.31,"amount":0.00376798},{"price":79970.22,"amount":0.0780364},{"price":79970.21,"amount":0.06756303}],
# "asks":[{"price":79983.3,"amount":0.00001385},{"price":79987.19,"amount":0.001},{"price":79989.47,"amount":0.02815556},{"price":79990,"amount":0.00001876},{"price":79991.77,"amount":0.003749},{"price":79991.78,"amount":0.423},{"price":79992,"amount":2},{"price":79992.77,"amount":0.39873019},{"price":79993.5,"amount":0.1246921},{"price":79994,"amount":0.4}],
# "timestamp":"2025-03-13T17:46:19.707970Z",
# "localTimestamp":"2025-03-13T17:46:20.004Z"}