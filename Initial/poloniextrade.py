import asyncio
import aiohttp
import json
import urllib.parse

async def run():
    """WebSocket client that connects and prints received data."""
    data_types = ["trade"] 

    stream_options = [
        {
            "exchange": "poloniex",
            "symbols": ["BTC_USDT"], # Coinbase uses hyphen in symbol
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
# {"type":"trade","symbol":"BTC_USDT","exchange":"poloniex",
#  "id":"126187411",
#  "price":80751.71,"amount":0.000181,
#  "side":"buy",
#  "timestamp":"2025-03-13T19:08:17.285Z",
#  "localTimestamp":"2025-03-13T19:08:17.398Z"}