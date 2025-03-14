import asyncio
import aiohttp
import json
import urllib.parse

async def run():
    """WebSocket client that connects and prints received data."""
    data_types = ["book_snapshot_10_100ms"] 

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
#{"type":"book_snapshot",
# "symbol":"BTC_USDT",
# "exchange":"poloniex",
# "name":"book_snapshot_10_100ms",
# "depth":10,"interval":100,
#"bids":[{"price":80050.06,"amount":0.00018},{"price":80035.37,"amount":0.000206},{"price":80010.02,"amount":0.000597},{"price":80010.01,"amount":0.14},{"price":80010,"amount":0.003212},{"price":79975.96,"amount":0.725814},{"price":79964.71,"amount":0.000597},{"price":79964.7,"amount":0.051206},{"price":79949.5,"amount":0.007196},{"price":79940,"amount":0.00254}],
#"asks":[{"price":80050.07,"amount":0.031362},{"price":80083.4,"amount":0.14},{"price":80096.12,"amount":0.19976},{"price":80104.13,"amount":0.19974},{"price":80112.14,"amount":0.19972},{"price":80118.36,"amount":0.746939},{"price":80119.6,"amount":0.041481},{"price":80120.28,"amount":0.955251},{"price":80123.8,"amount":1.306764},{"price":80134.11,"amount":0.007105}],
# "timestamp":"2025-03-13T17:57:08.152Z",
# "localTimestamp":"2025-03-13T17:57:08.227Z"}