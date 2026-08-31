import os
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import websocket
from binance.um_futures import UMFutures
from fastapi import FastAPI, HTTPException
import uvicorn

# ==================== CONFIG ====================
MAX_KLINES = 200
REST_WORKERS = 10
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
WS_URL = "wss://fstream.binance.com/market/stream"

# ==================== STATE ====================
kline_history = {}
cache_lock = threading.Lock()
closed_kline_count = 0
last_kline_time = 0

# ==================== FASTAPI APP ====================
app = FastAPI(title="Binance Base Data Daemon")

@app.get("/")
def root():
    with cache_lock:
        return {
            "status": "running",
            "symbols_loaded": len(kline_history),
            "closed_candles_count": closed_kline_count,
            "last_closed_time": last_kline_time
        }

@app.get("/candles")
def get_all_candles():
    with cache_lock:
        return kline_history

@app.get("/candles/{symbol}")
def get_symbol_candles(symbol: str):
    symbol = symbol.upper()
    with cache_lock:
        if symbol in kline_history:
            return {"symbol": symbol, "candles": kline_history[symbol]}
    raise HTTPException(status_code=404, detail="Symbol not found or not loaded yet")


# ==================== API / DATA ====================
def get_active_symbols():
    try:
        client = UMFutures()
        client.session.requests_params = {"timeout": 10}
        info = client.exchange_info()
        return [s["symbol"] for s in info["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["contractType"] == "PERPETUAL"]
    except Exception as e:
        print(f"[ERROR] Failed to fetch symbols: {e}")
        return []

def fetch_historical_klines(client, symbol):
    for attempt in range(3):
        try:
            klines = client.klines(symbol=symbol, interval="1m", limit=MAX_KLINES)
            if not klines:
                return symbol, None
            return symbol, [[int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[6])] for x in klines]
        except Exception as e:
            if attempt == 2:
                print(f"[ERROR] {symbol} failed: {e}")
            time.sleep(1)
    return symbol, None

def process_closed_kline(symbol, k):
    global closed_kline_count, last_kline_time
    new_kline = [int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]), int(k["T"])]
    
    with cache_lock:
        if symbol not in kline_history:
            kline_history[symbol] = []
        history = kline_history[symbol]
        
        if history and history[-1][0] == new_kline[0]:
            history[-1] = new_kline
        else:
            history.append(new_kline)
            
        if len(history) > MAX_KLINES:
            del history[:len(history) - MAX_KLINES]
            
        closed_kline_count += 1
        last_kline_time = time.time()

# ==================== WEBSOCKET ====================
def start_websocket(symbols):
    print("\n" + "="*60 + "\nSTARTING REALTIME WEBSOCKET\n" + "="*60)
    print(f"[WS] Symbols: {len(symbols)} | URL: {WS_URL}")

    def on_open(ws):
        print("[WS] Connection opened successfully.")
        streams = [f"{s.lower()}@kline_1m" for s in symbols]
        print(f"[WS] Subscribing to {len(streams)} kline streams...")
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

    def on_message(ws, message):
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            data = json.loads(message)
            
            if "result" in data:
                print(f"[WS] Subscribe response: {data}")
                return
            if "data" in data:
                data = data["data"]
            if data.get("e") != "kline":
                return
                
            symbol, kline = data.get("s"), data.get("k")
            if symbol and kline:
                process_closed_kline(symbol, kline)
        except Exception as e:
            print(f"[WS MESSAGE ERROR] {e}")

    def on_error(ws, error):
        print(f"[WS ERROR] {error}")

    def on_close(ws, code, message):
        print(f"[WS CLOSED] code={code}, message={message}")

    while True:
        try:
            print("[WS] Connecting...")
            ws = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=WS_PING_INTERVAL, ping_timeout=WS_PING_TIMEOUT)
        except Exception as e:
            print(f"[WS EXCEPTION] {e}")
        print("[WS] Reconnecting in 5 seconds...")
        time.sleep(5)

# ==================== MONITOR ====================
def status_monitor():
    global closed_kline_count, last_kline_time
    while True:
        time.sleep(10)
        with cache_lock:
            count, symbols_loaded = closed_kline_count, len(kline_history)
            total_candles = sum(len(x) for x in kline_history.values())
        age = "NO CANDLES YET" if last_kline_time == 0 else f"{time.time() - last_kline_time:.1f}s ago"
        print(f"[STATUS] Symbols: {symbols_loaded} | Candles in RAM: {total_candles:,} | Updates: {count} | Last: {age}")

# ==================== BACKGROUND DAEMON INIT ====================
def start_background_daemon():
    global kline_history
    print("="*60 + "\nBINANCE 1M CANDLE BASE DATA DAEMON\n" + "="*60)
    
    print("[INFO] Fetching active USDT perpetual symbols...")
    symbols = get_active_symbols()
    if not symbols:
        print("[ERROR] No active symbols found.")
        return

    print(f"[SUCCESS] Found {len(symbols)} active symbols.")

    print("\n" + "="*60 + "\nONE-TIME HISTORICAL DOWNLOAD\n" + "="*60)
    print(f"[INFO] Target: {len(symbols)} symbols × {MAX_KLINES} candles ({len(symbols) * MAX_KLINES:,} total)")

    client = UMFutures()
    client.session.requests_params = {"timeout": 10}
    loaded_count = 0
    failed_symbols = []
    progress_lock = threading.Lock()

    def worker(symbol):
        nonlocal loaded_count
        res_sym, hist = fetch_historical_klines(client, symbol)
        with progress_lock:
            loaded_count += 1
            print(f"\r[LOADING] {loaded_count}/{len(symbols)}", end="", flush=True)
        return res_sym, hist

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=REST_WORKERS) as executor:
        futures = [executor.submit(worker, s) for s in symbols]
        for f in as_completed(futures):
            sym, hist = f.result()
            if hist:
                with cache_lock:
                    kline_history[sym] = hist
            else:
                failed_symbols.append(sym)

    print(f"\n[SUCCESS] Download finished in {time.time() - start_time:.1f}s. Loaded: {len(kline_history)}/{len(symbols)}, Failed: {len(failed_symbols)}")
    print(f"[INFO] Candles in RAM: {sum(len(h) for h in kline_history.values()):,}")

    threading.Thread(target=status_monitor, daemon=True).start()
    start_websocket(symbols)


# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    # Background thread дээр дата татах болон websocket ажиллуулна
    daemon_thread = threading.Thread(target=start_background_daemon, daemon=True)
    daemon_thread.start()

    # Main thread дээр FastAPI (Uvicorn) серверийг асаана (Railway-н PORT-ыг ашиглана)
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
