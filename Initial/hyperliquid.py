#https://medium.com/tardis-dev/live-liquidations-monitor-for-top-cryptocurrency-exchanges-ac8e429e9556
#https://www.algos.org/p/small-trader-alpha-6-perpetual-arbitrage
'''l2Book:
Subscription message: { "type": "l2Book", "coin": "<coin_symbol>" }
Optional parameters: nSigFigs: int, mantissa: int
Data format: WsBook

trades:
Subscription message: { "type": "trades", "coin": "<coin_symbol>" }
Data format: WsTrade[]'''

import asyncio
import aiohttp
import json
import urllib.parse

async def run():
    """WebSocket client that connects and prints received data."""
    data_types = ["book_snapshot_10_100ms"] 

    stream_options = [
        {
            "exchange": "hyperliquid",
            "symbols": ["btc"], # Coinbase uses hyphen in symbol
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
# {"type":"book_snapshot","symbol":"BTC","exchange":"hyperliquid",
#  "name":"book_snapshot_10_100ms",
#  "depth":10,"interval":100,
#  "bids":[{"price":80600,"amount":0.22292},{"price":80599,"amount":1.20591},{"price":80598,"amount":2.9539},{"price":80597,"amount":0.75407},{"price":80595,"amount":0.36053},{"price":80594,"amount":0.12407},{"price":80593,"amount":0.49632},{"price":80592,"amount":0.78427},{"price":80589,"amount":1.1602},{"price":80588,"amount":5.60625}],
#  "asks":[{"price":80605,"amount":0.67962},{"price":80610,"amount":14.008},{"price":80611,"amount":2.03882},{"price":80612,"amount":0.12405},{"price":80613,"amount":7.67456},{"price":80614,"amount":2.53088},{"price":80615,"amount":5.13813},{"price":80616,"amount":0.63431},{"price":80617,"amount":0.24074},{"price":80618,"amount":0.6202}],
# "timestamp":"2025-03-13T18:21:51.685Z",
# "localTimestamp":"2025-03-13T18:21:51.921Z"}