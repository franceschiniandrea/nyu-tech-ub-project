import asyncio
import ccxt.pro as ccxt
import logging
import signal
from create_queue import order_book_queues_perps, trade_queues_perps
from timeutils import time_iso8601

from crypto_hft.utils.config import TARGET_TOKENS
from normalizers import normalize_symbol
from normalizers import normalize_order_book, normalize_trade

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- State ---
running_tasks = []
shutdown_event = asyncio.Event()

# --- Optional: Silence CancelledError from noisy CCXT internals ---
def silence_cancelled_errors(loop):
    def handler(loop, context):
        exc = context.get("exception")
        if isinstance(exc, asyncio.CancelledError):
            return  # suppress CancelledError spam
        loop.default_exception_handler(context)
    loop.set_exception_handler(handler)

def flatten_order_book(normalized):
    bids = normalized.pop("bids", [])
    asks = normalized.pop("asks", [])

    for i in range(15):
        bid = bids[i] if i < len(bids) else [0.0, 0.0]
        ask = asks[i] if i < len(asks) else [0.0, 0.0]

        normalized[f"bid_{i}_px"] = float(bid[0])
        normalized[f"bid_{i}_sz"] = float(bid[1])
        normalized[f"ask_{i}_px"] = float(ask[0])
        normalized[f"ask_{i}_sz"] = float(ask[1])

    return normalized


# --- Utils ---
def get_valid_symbol(exchange_id, token):
    base = token.upper()
    if exchange_id == "coinbase":
        return f"{base}/USDT:USDT"
    elif exchange_id == "hyperliquid":
        return f"{base}/USDC:USDC"
    elif exchange_id == "binance":
        return f"{base}/USDT"
    elif exchange_id == "poloniexfutures":
        return f"{base}/USDT:USDT"
    return None


async def get_symbols(exchange):
    await exchange.load_markets()
    return [
        s for t in TARGET_TOKENS
        if (s := get_valid_symbol(exchange.id, t)) in exchange.markets
    ]


# --- Streamers ---
async def stream_order_book(exchange, symbol):
    try:
        while not shutdown_event.is_set():
            ob = await exchange.watch_order_book(symbol)
            normalized = normalize_order_book(exchange.id, ob)

            if normalized:
                normalized["local_timestamp"] = time_iso8601()
                normalized = flatten_order_book(normalized)
                await order_book_queues_perps[normalize_symbol(normalized["symbol"])].put(normalized)

    except asyncio.CancelledError:
        logger.info(f"[ORDERBOOK] Cancelled: {exchange.id} {symbol}")
    except Exception as e:
        logger.warning(f"[ORDERBOOK] {exchange.id} {symbol} error: {e}")
        await asyncio.sleep(5)

async def stream_trades(exchange, symbol):
    try:
        while not shutdown_event.is_set():
            trades = await exchange.watch_trades(symbol)
            for trade in trades:
                normalized = normalize_trade(exchange.id, trade)
                if normalized:
                    normalized["local_timestamp"] = time_iso8601()
                    await trade_queues_perps[normalize_symbol(normalized["symbol"])].put(normalized)

                    #logger.info(f"[📥 TRADE QUEUE] {exchange.id} {symbol}")
    except asyncio.CancelledError:
        logger.info(f"[TRADES] Cancelled: {exchange.id} {symbol}")
    except Exception as e:
        logger.warning(f"[TRADES] {exchange.id} {symbol} error: {e}")
        await asyncio.sleep(5)


# --- Signal Handler ---
def handle_signal():
    logger.warning("🛑 Received shutdown signal. Cancelling tasks...")
    shutdown_event.set()
    for task in running_tasks:
        task.cancel()


# --- Main ---
async def main():
    exchanges = [
        ccxt.coinbase({'enableRateLimit': True}),
        ccxt.poloniexfutures({'enableRateLimit': True}),
        ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}})  
        # ccxt.hyperliquid({'enableRateLimit': True}),   # Add if needed
    ]

    # Register Ctrl+C / SIGTERM handler
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        for ex in exchanges:
            symbols = await get_symbols(ex)
            for sym in symbols:
                logger.info(f"[✅] Streaming {ex.id} {sym}")
                running_tasks.append(asyncio.create_task(stream_order_book(ex, sym)))
                running_tasks.append(asyncio.create_task(stream_trades(ex, sym)))

        await shutdown_event.wait()

    finally:
        logger.info("📦 Shutting down exchanges and cancelling tasks...")
        for task in running_tasks:
            task.cancel()
        await asyncio.gather(*running_tasks, return_exceptions=True)

        for ex in exchanges:
            try:
                await ex.close()
            except Exception as e:
                logger.warning(f"[{ex.id}] Close error: {e}")

        logger.info("✅ Shutdown complete.")


# --- Entry Point ---
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    silence_cancelled_errors(loop)  # Optional: Clean up CancelledError noise
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.warning("🛑 Interrupted manually")

