import asyncio
import aiohttp
import json
import urllib.parse

async def run():
    """WebSocket client that connects and prints received data."""
    data_types = ["book_snapshot_10_100ms"] 

    stream_options = [
        {
            "exchange": "binance-us",
            "symbols": ["btcusd"], 
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
#{"type":"book_snapshot","symbol":"BTCUSD","exchange":"binance-us",
# "name":"book_snapshot_10_100ms","depth":10,"interval":100,
# "bids":[{"price":80408.78,"amount":0.21513},{"price":80367.58,"amount":0.00145},{"price":80316.52,"amount":0.01273},{"price":80200,"amount":0.01209},{"price":80104.7,"amount":0.00624},{"price":80058.22,"amount":0.03361},{"price":80000.03,"amount":0.00062},{"price":80000.02,"amount":0.00025},{"price":80000.01,"amount":0.00125},{"price":80000,"amount":0.40839}],
# "asks":[{"price":80836.79,"amount":0.02045},{"price":80891.77,"amount":0.03975},{"price":81327.85,"amount":0.00126},{"price":81795.22,"amount":0.00083},{"price":81795.99,"amount":0.00083},{"price":81796,"amount":0.00166},{"price":81796.01,"amount":0.00083},{"price":81919.05,"amount":0.00126},{"price":82211,"amount":0.0296},{"price":82514.55,"amount":0.00125}],
# "timestamp":"2025-03-13T18:06:10.521Z",
# "localTimestamp":"2025-03-13T18:06:10.521Z"}