import asyncio
import aiohttp
import json
import urllib.parse

base_tickers = [
    #"OP_USDT",  #Optimism (OP)	
    #"SC_USDT", #Siacoin (SC)	
    #"LDO_USDT", #Lido DAO (LDO)	
    #"SUI_USDT", #Sui (SUI)	
    #"NEAR_USDT", #NEAR Protocol (NEAR)	
    #"DASH_USDT", #Dash (DASH)	
    #"ATOM_USDT", #Cosmos (ATOM)	
    #"STEEM_USDT", #Steem (STEEM)	
    #"UNI_USDT", #Uniswap (UNI)	
    #"PEPE_USDT", #Pepe (PEPE)	
    #"BNB_USDT", #Binance Coin (BNB)	
    #"LINK_USDT", #Chainlink (LINK)	
    "BTC_USDT", #Bitcoin (BTC)
    #"ETH_USDT" #Ethereum (ETH)
]

def map_symbols(exchange, tickers):
    mapped_symbols = []
    for ticker in tickers:
        base, quote = ticker.split("_")  

        if exchange == "hyperliquid":
            mapped_symbols.append(base.lower())  # Hyperliquid uses lowercase base (e.g., "btc", "eth")
        
        elif exchange == "binance-us":
            mapped_symbols.append(f"{base.lower()}{quote.lower()}")  # Binance US uses lowercase merged (e.g., "btcusdt", "ethusdt")
        
        elif exchange == "poloniex":
            mapped_symbols.append(f"{base}_{quote}")  # Poloniex uses "BASE_QUOTE" format (e.g., "BTC_USDT")
        
        elif exchange == "coinbase":
            mapped_symbols.append(f"{base}-{quote.replace('USDT', 'USD')}")  # Coinbase uses "BASE-USD" (e.g., "BTC-USD")

    return mapped_symbols

async def run():
    """WebSocket client that dynamically maps symbols for multiple exchanges."""
    data_types = ["trade"]
    
    stream_options = [
        {
            "exchange": exchange,
            "symbols": map_symbols(exchange, base_tickers),
            "dataTypes": data_types
        }
        for exchange in ["hyperliquid",  "poloniex", "coinbase"]#"binance-us",
    ]
    
    options = urllib.parse.quote_plus(json.dumps(stream_options))
    URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(URL) as websocket:
            print("[+] Connected to WebSocket. Receiving data...\n")

            async for msg in websocket:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    print(msg.data)
                elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    print("[!] WebSocket closed. Reason:", msg.data)
                    break  

asyncio.run(run())
