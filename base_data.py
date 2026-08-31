import os
import json
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import websocket
from binance.um_futures import UMFutures
from fastapi import FastAPI, HTTPException
import uvicorn
import pandas as pd
from ta.momentum import RSIIndicator

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

@app.get("/rsi/{symbol}")
def get_symbol_rsi(symbol: str):
    symbol = symbol.upper()
    with cache_lock:
        if symbol not in kline_history:
            raise HTTPException(status_code=404, detail="Symbol not found or not loaded yet")
        klines = kline_history[symbol]

    # RSI тооцоолох цэвэрхэн функц рүү датагаа явуулж байна
    result = calculate_rsi_report(klines, symbol)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
    
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

# ==================== RSI CALCULATOR FUNCTION ====================
# ==================== ADVANCED RSI CALCULATION FUNCTION ====================
def calculate_rsi_report(klines, symbol="UNKNOWN"):
    try:
        if not klines or len(klines) < 50:
            return {"error": "Not enough kline data for RSI"}

        closes = [float(x[4]) for x in klines]
        closes_series = pd.Series(closes)

        # RSI Тооцоолол
        rsi_series = RSIIndicator(close=closes_series, window=7).rsi().dropna()
        if len(rsi_series) < 4:
            return {"error": "Insufficient RSI data"}

        rsi0, rsi1, rsi2, rsi3 = (
            rsi_series.iloc[-1],
            rsi_series.iloc[-2],
            rsi_series.iloc[-3],
            rsi_series.iloc[-4],
        )

        offset = len(klines) - len(rsi_series)
        price_history = {
            "rsi_30_up": [],
            "rsi_30_down": [],
            "rsi_70_up": [],
            "rsi_70_down": [],
        }

        # Last status
        last_status = "None"
        for i in range(len(rsi_series) - 1, 0, -1):
            prev_rsi, curr_rsi = rsi_series.iloc[i - 1], rsi_series.iloc[i]
            if prev_rsi <= 30 and curr_rsi > 30:
                last_status = "30U"
                break
            if prev_rsi >= 30 and curr_rsi < 30:
                last_status = "30D"
                break
            if prev_rsi <= 70 and curr_rsi > 70:
                last_status = "70U"
                break
            if prev_rsi >= 70 and curr_rsi < 70:
                last_status = "70D"
                break

        # Trend logic
        trend_status_history = deque(maxlen=10)
        for i in range(len(rsi_series) - 1, 0, -1):
            prev_rsi, curr_rsi = rsi_series.iloc[i - 1], rsi_series.iloc[i]
            new_status = None
            if prev_rsi <= 30 and curr_rsi > 30:
                new_status = "30U"
            elif prev_rsi >= 30 and curr_rsi < 30:
                new_status = "30D"
            elif prev_rsi <= 50 and curr_rsi > 50:
                new_status = "50U"
            elif prev_rsi >= 50 and curr_rsi < 50:
                new_status = "50D"
            elif prev_rsi <= 70 and curr_rsi > 70:
                new_status = "70U"
            elif prev_rsi >= 70 and curr_rsi < 70:
                new_status = "70D"
            if new_status:
                if not trend_status_history or trend_status_history[0] != new_status:
                    trend_status_history.appendleft(new_status)

        trend = "None"
        if len(trend_status_history) >= 2:
            for i in range(len(trend_status_history) - 1, 0, -1):
                curr, prev = trend_status_history[i], trend_status_history[i - 1]
                if prev == "30U" and curr == "50U":
                    trend = "uptrand1"
                    break
                elif prev == "50U" and curr == "70U":
                    trend = "uptrand2"
                    break
                elif prev == "70D" and curr == "50D":
                    trend = "downtrand1"
                    break
                elif prev == "50D" and curr == "30D":
                    trend = "downtrand2"
                    break

        # Historical crossover prices
        for i in range(len(rsi_series) - 2, 2, -1):
            prev_rsi, cur_rsi = rsi_series.iloc[i - 1], rsi_series.iloc[i]
            window_klines = klines[i + offset - 3 : i + offset + 1]
            w_opens = [float(k[1]) for k in window_klines]
            if prev_rsi <= 30 and cur_rsi > 30:
                price_history["rsi_30_up"].append(min(w_opens))
            if prev_rsi >= 30 and cur_rsi < 30:
                price_history["rsi_30_down"].append(max(w_opens))
            if prev_rsi <= 70 and cur_rsi > 70:
                price_history["rsi_70_up"].append(min(w_opens))
            if prev_rsi >= 70 and cur_rsi < 70:
                price_history["rsi_70_down"].append(max(w_opens))

        s30u_val = price_history["rsi_30_up"][0] if price_history["rsi_30_up"] else None
        s70d_val = price_history["rsi_70_down"][0] if price_history["rsi_70_down"] else None
        s70u_val = price_history["rsi_70_up"][0] if price_history["rsi_70_up"] else None
        s30d_val = price_history["rsi_30_down"][0] if price_history["rsi_30_down"] else None

        average_status = (s30u_val + s70d_val) / 2.0 if s30u_val and s70d_val else None
        valid_up = [v for v in [s30u_val, s70u_val] if v is not None]
        valid_down = [v for v in [s30d_val, s70d_val] if v is not None]
        max_u = max(valid_up) if valid_up else None
        min_d = min(valid_down) if valid_down else None

        return {
            "symbol": symbol,
            "values": {"0": rsi0, "-1": rsi1, "-2": rsi2, "-3": rsi3},
            "last_status": last_status,
            "trend": trend,
            "average_status": average_status,
            "MAXU": max_u,
            "MIND": min_d,
            "cross_history": {
                "s30u": s30u_val,
                "s30u_prev": price_history["rsi_30_up"][1] if len(price_history["rsi_30_up"]) > 1 else None,
                "s30d": s30d_val,
                "s30d_prev": price_history["rsi_30_down"][1] if len(price_history["rsi_30_down"]) > 1 else None,
                "s70u": s70u_val,
                "s70u_prev": price_history["rsi_70_up"][1] if len(price_history["rsi_70_up"]) > 1 else None,
                "s70d": s70d_val,
                "s70d_prev": price_history["rsi_70_down"][1] if len(price_history["rsi_70_down"]) > 1 else None,
            },
            "cross_now": {
                "30_up": "UP" if (rsi2 <= 30 and rsi1 > 30) else "--",
                "70_up": "UP" if (rsi2 <= 70 and rsi1 > 70) else "--",
                "30_down": "DOWN" if (rsi2 >= 30 and rsi1 < 30) else "--",
                "70_down": "DOWN" if (rsi2 >= 70 and rsi1 < 70) else "--",
            },
        }
    except Exception as e:
        return {"error": str(e)}

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    # 1. Background thread дээр дата татах болон websocket-ийг асаана
    daemon_thread = threading.Thread(target=start_background_daemon, daemon=True)
    daemon_thread.start()

    # 2. Main thread дээр Uvicorn (FastAPI) серверийг асаана
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
