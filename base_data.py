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
import requests
from collections import deque
import datetime

# ==================== CONFIG ====================
MAX_KLINES = 300  # MACD болон RSI-д хангалттай түүхэн дата
REST_WORKERS = 10
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
WS_URL = "wss://fstream.binance.com/market/stream"

# ==================== STATE ====================
kline_history = {}
macd_state = {}
ohlc_states = {}
cache_lock = threading.Lock()
closed_kline_count = 0
last_kline_time = 0
# ==================== FASTAPI APP ====================
app = FastAPI(title="Binance Base Data Daemon with RSI & MACD Logic")

@app.get("/railway-ip")
def get_railway_public_ip():
    """Railway серверийн гадагшаа гарч буй Public IP-г шалгах"""
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=5)
        return response.json()
    except Exception as e:
        return {"error": str(e)}
        
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

    result = calculate_rsi_report(klines, symbol)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.get("/macd/{symbol}")
def get_symbol_macd(symbol: str):
    symbol = symbol.upper()
    with cache_lock:
        if symbol not in kline_history:
            raise HTTPException(status_code=404, detail="Symbol not found or not loaded yet")
        klines = kline_history[symbol]

    result = calculate_macd_report(klines, symbol)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

# ==================== TOP GAINERS & LOSERS REPORT ====================
@app.get("/top-movers")
def calculate_gain_lose_report():
    global kline_history, macd_state
    movers_list = []

    with cache_lock:
        for symbol, klines in kline_history.items():
            if not klines or len(klines) < 50:
                continue
            
            closes = [float(x[4]) for x in klines]
            closes_series = pd.Series(closes)

            ema12 = closes_series.ewm(span=12, adjust=False).mean()
            ema26 = closes_series.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            macd_signal = macd_line.ewm(span=9, adjust=False).mean()

            # Хуучин state байгаа эсэхээс үл хамааран хүсэлт бүрт шинэчлэн тооцоолох (Логикийг огт эвдээгүй)
            macd_state[symbol] = _build_initial_macd_state(klines, macd_line, macd_signal)
            
            init_price = macd_state[symbol].get("macd_initial_price")
            if not init_price or init_price <= 0:
                init_price = float(klines[-1][1])

            try:
                # Энд сүүлийн лааны close ханшийг авч байна
                close_price = float(klines[-1][4])
            except (IndexError, ValueError):
                continue

            change_percent = ((close_price - init_price) / init_price) * 100

            movers_list.append({
                "symbol": symbol,
                "initial_price": round(init_price, 8),
                "close_price": round(close_price, 8),
                "change_percent": round(change_percent, 2)
            })

    if not movers_list:
        return {"error": "No valid data calculated yet"}

    sorted_by_gain = sorted(movers_list, key=lambda x: x["change_percent"], reverse=True)

    return {
        "top_gainers": sorted_by_gain[:10],
        "top_losers": sorted_by_gain[-10:][::-1]
    }

@app.get("/ohlc/{symbol}")
def get_symbol_ohlc(symbol: str):
    symbol = symbol.upper()
    with cache_lock:
        if symbol not in kline_history:
            raise HTTPException(status_code=404, detail="Symbol not found or not loaded yet")
        klines = kline_history[symbol]

    result = calculate_ohlc_tracker_report(klines, symbol)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.get("/all/{symbol}")
def get_symbol_all_data(symbol: str):
    symbol = symbol.upper()
    with cache_lock:
        if symbol not in kline_history:
            raise HTTPException(status_code=404, detail="Symbol not found or not loaded yet")
        klines = kline_history[symbol]

    rsi_res = calculate_rsi_report(klines, symbol)
    macd_res = calculate_macd_report(klines, symbol)
    ohlc_res = calculate_ohlc_tracker_report(klines, symbol)

    # Тухайн койны gain/lose хувийг тооцоолох
    tops_res = None
    try:
        if symbol in macd_state:
            init_price = macd_state[symbol].get("macd_initial_price")
            if init_price and init_price > 0:
                close_price = float(klines[-1][4])
                change_percent = ((close_price - init_price) / init_price) * 100
                tops_res = {
                    "symbol": symbol,
                    "initial_price": round(init_price, 8),
                    "close_price": round(close_price, 8),
                    "change_percent": round(change_percent, 2)
                }
    except Exception:
        pass

    return {
        "symbol": symbol,
        "rsi": rsi_res if "error" not in rsi_res else None,
        "macd": macd_res if "error" not in macd_res else None,
        "ohlc": ohlc_res if "error" not in ohlc_res else None,
        "tops": tops_res
    }

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
        print("[WS] Reconnecting in 5 seconds নমুনা...")
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

# ==================== ADVANCED RSI CALCULATION FUNCTION ====================
def calculate_rsi_report(klines, symbol="UNKNOWN"):
    try:
        if not klines or len(klines) < 50:
            return {"error": "Not enough kline data for RSI"}

        closes = [float(x[4]) for x in klines]
        closes_series = pd.Series(closes)

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

# ==================== ADVANCED MACD CALCULATION ====================
import datetime

def _build_initial_macd_state(klines, macd_line, macd_signal):
    initial_st = {
        "uplimit": None, "downlimit": None,
        "uplimit_cross_line": None, "downlimit_cross_line": None,
        "trend": "None",
        "macd_initial_price": None,
        "macd_initial_time": None,      # <--- MACD цагийг хадгалах түлхүүр
        "signal_initial_price": None,
        "signal_initial_time": None     # <--- Signal цагийг хадгалах түлхүүр
    }
    
    # 1. Trend болон limit-уудыг олох хэсэг (хэвээрээ)
    last_cross_up_idx = -1
    last_cross_down_idx = -1

    for i in range(len(macd_line) - 2, 2, -1):
        if macd_line.iloc[i] > macd_signal.iloc[i] and macd_line.iloc[i-1] < macd_signal.iloc[i-1]:
            if initial_st["uplimit"] is None:
                last_cross_up_idx = i
                window_klines = macd_line.iloc[i-3:i+1]
                initial_st["uplimit"] = min(window_klines)
                initial_st["uplimit_cross_line"] = macd_line.iloc[i]

        if macd_line.iloc[i] < macd_signal.iloc[i] and macd_line.iloc[i-1] > macd_signal.iloc[i-1]:
            if initial_st["downlimit"] is None:
                last_cross_down_idx = i
                window_klines = macd_line.iloc[i-3:i+1]
                initial_st["downlimit"] = max(window_klines)
                initial_st["downlimit_cross_line"] = macd_line.iloc[i]

        if initial_st["uplimit"] is not None and initial_st["downlimit"] is not None:
            break

    if last_cross_up_idx > last_cross_down_idx:
        initial_st["trend"] = "UP"
    elif last_cross_down_idx > last_cross_up_idx:
        initial_st["trend"] = "DOWN"

    offset = len(klines) - len(macd_line)

    # 2. MACD LINE Zero Cross & Timestamp
    found_zero_cross = False
    for i in range(len(macd_line) - 1, 0, -1):
        curr_line = macd_line.iloc[i]
        prev_line = macd_line.iloc[i-1]
        if (prev_line > 0 and curr_line < 0) or (prev_line < 0 and curr_line > 0):
            kline_idx = i + offset
            if 0 <= kline_idx < len(klines):
                initial_st["macd_initial_price"] = float(klines[kline_idx][1])
                # Millisecond-ийг уншигдахуйц цаг болгон хөрвүүлэх (UTC эсвэл local)
                ts_ms = int(klines[kline_idx][0])
                initial_st["macd_initial_time"] = datetime.datetime.fromtimestamp(ts_ms / 1000.0, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                found_zero_cross = True
                break

    if not found_zero_cross and klines:
        initial_st["macd_initial_price"] = float(klines[-1][1])
        ts_ms = int(klines[-1][0])
        initial_st["macd_initial_time"] = datetime.datetime.fromtimestamp(ts_ms / 1000.0, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # 3. SIGNAL LINE Zero Cross & Timestamp
    found_signal_zero_cross = False
    for i in range(len(macd_signal) - 1, 0, -1):
        curr_sig = macd_signal.iloc[i]
        prev_sig = macd_signal.iloc[i-1]
        if (prev_sig > 0 and curr_sig < 0) or (prev_sig < 0 and curr_sig > 0):
            kline_idx = i + offset
            if 0 <= kline_idx < len(klines):
                initial_st["signal_initial_price"] = float(klines[kline_idx][1])
                ts_ms = int(klines[kline_idx][0])
                initial_st["signal_initial_time"] = datetime.datetime.fromtimestamp(ts_ms / 1000.0, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                found_signal_zero_cross = True
                break

    if not found_signal_zero_cross and klines:
        initial_st["signal_initial_price"] = float(klines[-1][1])
        ts_ms = int(klines[-1][0])
        initial_st["signal_initial_time"] = datetime.datetime.fromtimestamp(ts_ms / 1000.0, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    return initial_st

def calculate_macd_report(klines, symbol="UNKNOWN"):
    global macd_state
    try:
        if not klines or len(klines) < 50:
            return {"error": "Not enough kline data for MACD"}

        closes = [float(x[4]) for x in klines]
        closes_series = pd.Series(closes)

        ema12 = closes_series.ewm(span=12, adjust=False).mean()
        ema26 = closes_series.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal

        if len(macd_line.dropna()) < 4:
            return {"error": "Insufficient MACD data"}

        # Хуучин state байгаа эсэхээс үл хамааран хүсэлт бүрт шинэчлэн тооцоолох
        macd_state[symbol] = _build_initial_macd_state(klines, macd_line, macd_signal)

        if symbol not in macd_state:
            macd_state[symbol]["line_direction"] = "None"
            macd_state[symbol]["macd_lineup_limit"] = None
            macd_state[symbol]["macd_linedown_limit"] = None

        macd_up = macd_line.iloc[-2] > macd_signal.iloc[-2] and macd_line.iloc[-3] < macd_signal.iloc[-3]
        macd_down = macd_line.iloc[-2] < macd_signal.iloc[-2] and macd_line.iloc[-3] > macd_signal.iloc[-3]

        line_minus_1 = macd_line.iloc[-2]
        line_minus_2 = macd_line.iloc[-3]

        current_line_direction = "UP" if line_minus_1 > line_minus_2 else "DOWN"
        previous_line_direction = macd_state[symbol].get("line_direction", "None")

        if current_line_direction != previous_line_direction:
            if current_line_direction == "DOWN":
                macd_state[symbol]["macd_lineup_limit"] = line_minus_2
            elif current_line_direction == "UP":
                macd_state[symbol]["macd_linedown_limit"] = line_minus_2
        
        macd_state[symbol]["line_direction"] = current_line_direction

        last_4_lines = [macd_line.iloc[-1], macd_line.iloc[-2], macd_line.iloc[-3], macd_line.iloc[-4]]
        macd_min = min(last_4_lines)
        macd_max = max(last_4_lines)

        if macd_up:
            macd_state[symbol]["trend"] = "UP"
            macd_state[symbol]["uplimit"] = macd_min
            macd_state[symbol]["uplimit_cross_line"] = macd_line.iloc[-2]
        if macd_down:
            macd_state[symbol]["trend"] = "DOWN"
            macd_state[symbol]["downlimit"] = macd_max
            macd_state[symbol]["downlimit_cross_line"] = macd_line.iloc[-2]

        current_trend = macd_state[symbol].get("trend", "None")

        return {
            "symbol": symbol,
            "line": {
                "0": f"{macd_line.iloc[-1]:.8f}",
                "-1": f"{macd_line.iloc[-2]:.8f}",
                "-2": f"{macd_line.iloc[-3]:.8f}",
                "-3": f"{macd_line.iloc[-4]:.8f}"
            },
            "signal": {
                "0": f"{macd_signal.iloc[-1]:.8f}",
                "-1": f"{macd_signal.iloc[-2]:.8f}",
                "-2": f"{macd_signal.iloc[-3]:.8f}",
                "-3": f"{macd_signal.iloc[-4]:.8f}"
            },
            "hist": {
                "0": f"{macd_hist.iloc[-1]:.8f}",
                "-1": f"{macd_hist.iloc[-2]:.8f}",
                "-2": f"{macd_hist.iloc[-3]:.8f}",
                "-3": f"{macd_hist.iloc[-4]:.8f}"
            },
            "macd_up": current_trend == "UP",
            "macd_down": current_trend == "DOWN",
            "macd_min": f"{macd_min:.8f}",
            "macd_max": f"{macd_max:.8f}",
            "macd_uplimit": f"{macd_state[symbol]['uplimit']:.8f}" if macd_state[symbol]['uplimit'] is not None else None,
            "macd_downlimit": f"{macd_state[symbol]['downlimit']:.8f}" if macd_state[symbol]['downlimit'] is not None else None,
            "macd_lineup_limit": f"{macd_state[symbol].get('macd_lineup_limit'):.8f}" if macd_state[symbol].get('macd_lineup_limit') is not None else None,
            "macd_linedown_limit": f"{macd_state[symbol].get('macd_linedown_limit'):.8f}" if macd_state[symbol].get('macd_linedown_limit') is not None else None,
            "uplimit_cross_line": f"{macd_state[symbol]['uplimit_cross_line']:.8f}" if macd_state[symbol]['uplimit_cross_line'] is not None else None,
            "downlimit_cross_line": f"{macd_state[symbol]['downlimit_cross_line']:.8f}" if macd_state[symbol]['downlimit_cross_line'] is not None else None,
            "macd_initial_price": f"{macd_state[symbol].get('macd_initial_price'):.8f}" if macd_state[symbol].get('macd_initial_price') is not None else None,
            "macd_initial_time": macd_state[symbol].get('macd_initial_time'),
            "signal_initial_price": f"{macd_state[symbol].get('signal_initial_price'):.8f}" if macd_state[symbol].get('signal_initial_price') is not None else None,
            "signal_initial_time": macd_state[symbol].get('signal_initial_time')
        }
    except Exception as e:
        return {"error": str(e)}

# ==================== ADVANCED OHLC TRACKER ====================
def calculate_ohlc_tracker_report(klines, symbol="UNKNOWN"):
    global ohlc_states
    try:
        if not klines or len(klines) < 4:
            return {"error": "Not enough kline data for OHLC tracker"}

        last_4 = klines[-4:]
        index_keys = ["-3", "-2", "-1", "0"]
        ohlc_dict = {}
        ohlc_list_for_df = []

        for i in range(4):
            kline = last_4[i]
            idx_key = index_keys[i]
            candle_data = {
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4])
            }
            ohlc_dict[idx_key] = candle_data
            ohlc_list_for_df.append(candle_data)

        df = pd.DataFrame(ohlc_list_for_df)

        min_open = df["open"].min()
        max_open = df["open"].max()
        min_close = df["close"].min()
        max_close = df["close"].max()
        max_high = df["high"].max()
        min_low = df["low"].min()

        open0 = ohlc_list_for_df[-1]["open"]
        open1 = ohlc_list_for_df[-2]["open"]

        openup = open0 > open1
        opendown = open0 < open1

        if symbol not in ohlc_states:
            ohlc_states[symbol] = {
                "last_openup_limit": None,
                "last_opendown_limit": None,
                "prev_openup": False,
                "prev_opendown": False
            }

        st = ohlc_states[symbol]

        if openup and not st["prev_openup"]:
            st["last_openup_limit"] = min_open

        if opendown and not st["prev_opendown"]:
            st["last_opendown_limit"] = max_open

        st["prev_openup"] = openup
        st["prev_opendown"] = opendown

        return {
            "symbol": symbol,
            "candles": ohlc_dict,
            "min_open": min_open,
            "max_open": max_open,
            "min_close": min_close,
            "max_close": max_close,
            "max_high": max_high,
            "min_low": min_low,
            "openup": openup,
            "opendown": opendown,
            "openup_limit": st["last_openup_limit"],
            "opendown_limit": st["last_opendown_limit"]
        }
    except Exception as e:
        return {"error": str(e)}

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    daemon_thread = threading.Thread(target=start_background_daemon, daemon=True)
    daemon_thread.start()

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

# ==================== FASTAPI STARTUP EVENT ====================
@app.on_event("startup")
def startup_event():
    """FastAPI сервер асахад дата татах болон WebSocket background daemon-ийг автоматаар эхлүүлэх"""
    daemon_thread = threading.Thread(target=start_background_daemon, daemon=True)
    daemon_thread.start()
    print("🚀 FastAPI startup event: Background data daemon started successfully.")


# ==================== BOT CONTROL & INTEGRATION ====================
bot_is_running = False
bot_thread = None
bot_lock = threading.Lock()

def trading_bot_loop():
    """Арилжааны ботын үндсэн цикл (Railway сервер дээр 24/7 ажиллана)"""
    global bot_is_running
    print("🤖 [BOT] Арилжааны бот сервер дотор эхэллээ...")
    
    try:
        import client
        get_client = client.get_client
        get_open_positions = client.get_open_positions
        get_symbol_rules = client.get_symbol_rules
        open_long_position = client.open_long_position
        open_short_position = client.open_short_position
        close_long_position = client.close_long_position
        close_short_position = client.close_short_position
        
        client_inst = get_client()
        if not client_inst:
            print("🔥 [BOT ERROR] Binance API client үүсгэхэд алдаа гарлаа (API Key шалгана уу). Бот зогслоо.")
            bot_is_running = False
            return
    except Exception as e:
        print(f"🔥 [BOT ERROR] client.py импортлоход алдаа гарлаа: {e}")
        bot_is_running = False
        return

    ACTIVE_TOP_N = 1
    LIMITS_FILE = "trade_data.json"
    BASELINE_FILE = "baseline.json"

    def read_json_file(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except Exception:
            return {}

    def write_json_file(filename, data):
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def read_baseline_file():
        return read_json_file(BASELINE_FILE)

    def write_baseline_file(baselines):
        write_json_file(BASELINE_FILE, baselines)

    while bot_is_running:
        try:
            all_open_positions = client.get_open_positions(client_inst)

            if not all_open_positions:
                write_json_file(LIMITS_FILE, {})

            movers = calculate_gain_lose_report()
            if "error" in movers:
                time.sleep(3)
                continue

            top_gainers_raw = movers.get("top_gainers", [])
            top_losers_raw = movers.get("top_losers", [])

            active_gainers = [item["symbol"] for item in top_gainers_raw[:ACTIVE_TOP_N]]
            active_losers = [item["symbol"] for item in top_losers_raw[:ACTIVE_TOP_N]]

            position_symbols = {pos["symbol"] for pos in all_open_positions.values()}
            symbols_to_check = sorted(set(active_gainers) | set(active_losers) | position_symbols)

            for symbol in symbols_to_check:
                if not bot_is_running:
                    break
                try:
                    klines = kline_history.get(symbol)
                    if not klines or len(klines) < 50:
                        continue

                    rsi_res = calculate_rsi_report(klines, symbol)
                    macd_res = calculate_macd_report(klines, symbol)
                    ohlc_res = calculate_ohlc_tracker_report(klines, symbol)

                    if "error" in macd_res or "error" in ohlc_res:
                        continue

                    macd_up = macd_res.get("macd_up")
                    macd_down = macd_res.get("macd_down")
                    candles = ohlc_res.get("candles", {})
                    open0 = float(candles.get("0", {}).get("open", 0))
                    open1 = float(candles.get("-1", {}).get("open", 0))
                    openup_limit = ohlc_res.get("openup_limit")
                    opendown_limit = ohlc_res.get("opendown_limit")

                    long_opened = f"{symbol}_LONG" in all_open_positions
                    short_opened = f"{symbol}_SHORT" in all_open_positions

                    rules = client.get_symbol_rules(client_inst, symbol)
                    if not rules:
                        continue
                        
                    is_gainer = symbol in active_gainers
                    is_loser = symbol in active_losers

                    def check_and_reset_baseline(condition_name):
                        try:
                            baselines = read_baseline_file()
                            new_macd_init_price = float(macd_res.get("macd_initial_price", 0))
                            if new_macd_init_price > 0:
                                baselines[symbol] = new_macd_init_price
                                write_baseline_file(baselines)
                        except Exception:
                            pass

                    if is_gainer:
                        # HEDGE LONG хаах
                        if long_opened and open0 < open1 and open0 < openup_limit:
                            check_and_reset_baseline("Gainer Long Close Limit")
                            pos_info = all_open_positions.get(f"{symbol}_LONG")
                            if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                                result = close_long_position(client_inst, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)
                                # Үр дүнг энд шалгаж болно

                        # LONG нээх
                        if not long_opened and macd_up and open0 > open1:
                            check_and_reset_baseline("Gainer Long Open Limit")
                            result = open_long_position(client_inst, symbol, info=rules)

                    elif is_loser:
                        # HEDGE SHORT хаах
                        if short_opened and open0 > open1 and open0 > opendown_limit:
                            check_and_reset_baseline("Loser Short Close Limit")
                            pos_info = all_open_positions.get(f"{symbol}_SHORT")
                            if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                                result = close_short_position(client_inst, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)

                        # SHORT нээх
                        if not short_opened and macd_down and open0 < open1:
                            check_and_reset_baseline("Loser Short Open Limit")
                            result = open_short_position(client_inst, symbol, info=rules)

                    else:
                        # Жагсаалтаас гарсан LONG хаах
                        if long_opened and not is_gainer and open0 < open1 and open0 < openup_limit:
                            check_and_reset_baseline("Out-of-list Long Close Limit")
                            pos_info = all_open_positions.get(f"{symbol}_LONG")
                            if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                                result = close_long_position(client_inst, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)

                        # Жагсаалтаас гарсан SHORT хаах
                        if short_opened and not is_loser and open0 > open1 and open0 > opendown_limit:
                            check_and_reset_baseline("Out-of-list Short Close Limit")
                            pos_info = all_open_positions.get(f"{symbol}_SHORT")
                            if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                                result = close_short_position(client_inst, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)

                except Exception as e:
                    print(f"🔥 [SYMBOL ERROR] {symbol}: {e}")

                if not bot_is_running:
                    break
                time.sleep(1)

        except Exception as e:
            print(f"🔥 [BOT LOOP ERROR]: {e}")
            time.sleep(5)

    print("🛑 [BOT] Арилжааны бот амжилттай зогслоо.")

@app.get("/bot/start")
def start_bot():
    global bot_is_running, bot_thread
    with bot_lock:
        if bot_is_running:
            return {"status": "already running"}
        bot_is_running = True
        bot_thread = threading.Thread(target=trading_bot_loop, daemon=True)
        bot_thread.start()
    return {"status": "bot started successfully"}

@app.get("/bot/stop")
def stop_bot():
    global bot_is_running
    with bot_lock:
        if not bot_is_running:
            return {"status": "already stopped"}
        bot_is_running = False
    return {"status": "bot stop signal sent"}

@app.get("/bot/status")
def bot_status():
    return {"is_running": bot_is_running}
