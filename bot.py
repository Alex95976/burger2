import os
import json
import time
import traceback
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# client.py-аас биржийн захиалга болон удирдах функцүүдийг импортлох
from client import (get_client, get_open_positions, get_symbol_rules,
                     open_long_position, open_short_position,
                     close_long_position, close_short_position)

# Railway сервер дээрх дата API хаяг (Environment variable эсвэл шууд линк)
RAILWAY_API_URL = os.getenv("RAILWAY_API_URL", "https://burger2-production.up.railway.app")

ACTIVE_TOP_N = 5  # <--- Top 1 эсвэл Top 5 гэж удирдах
file_lock = threading.Lock()

def get_railway_top_movers():
    """Railway серверээс топ өсөлт/уналттай койнуудын жагсаалтыг татах"""
    try:
        response = requests.get(f"{RAILWAY_API_URL}/top-movers", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return data.get("top_gainers", []), data.get("top_losers", [])
    except Exception as e:
        print(f"⚠️ Top movers татахад алдаа гарлаа: {e}")
    return [], []

def get_all_indicator_data_from_railway(symbol):
    """Тухайн койны RSI, MACD, OHLC болон бусад бүх индикаторыг Railway серверээс авах"""
    try:
        response = requests.get(f"{RAILWAY_API_URL}/all/{symbol}", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"⚠️ [{symbol}] Railway серверээс дата татахад алдаа гарлаа: {e}")
    return None

# --- Позицын limit утгыг хадгалах, унших функцүүд ---
LIMITS_FILE = "trade_data.json"
BASELINE_FILE = "baseline.json"

def read_json_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return {}

def write_json_file(filename, data):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        pass

def read_position_limits():
    return read_json_file(LIMITS_FILE)

def write_position_limits(limits):
    write_json_file(LIMITS_FILE, limits)

def read_baseline_file():
    return read_json_file(BASELINE_FILE)

def write_baseline_file(baselines):
    write_json_file(BASELINE_FILE, baselines)

def update_limit(symbol, side, value):
    limits = read_position_limits()
    if symbol not in limits:
        limits[symbol] = {}
    limits[symbol][side] = value
    write_position_limits(limits)

def update_macd_info(symbol, uplimit, downlimit, line1, line2):
    def format_val(v):
        if v is None:
            return None
        return float(f"{float(v):.8f}") if abs(float(v)) >= 0.0001 else float(f"{float(v):.10f}")

    limits = read_position_limits()
    if symbol not in limits:
        limits[symbol] = {}
        
    limits[symbol]["macd_uplimit"] = format_val(uplimit)
    limits[symbol]["macd_downlimit"] = format_val(downlimit)
    limits[symbol]["macd_line1"] = format_val(line1)
    limits[symbol]["macd_line2"] = format_val(line2)
    
    write_position_limits(limits)

def update_market_and_pnl_status(client, top_gainers, top_losers, all_open_positions, baseline_time=None):
    limits = read_position_limits()
    new_limits = {}

    total_margin = 0.0
    positions_summary = {}

    for pos_key, pos_val in all_open_positions.items():
        try:
            symbol = pos_val.get("symbol")
            position_amt = abs(float(pos_val.get("positionAmt", 0)))
            entry_price = float(pos_val.get("entryPrice", 0))
            unrealized_profit = pos_val.get("unRealizedProfit")
            
            notional_value = position_amt * entry_price
            
            leverage = 20.0
            if symbol:
                try:
                    rules = get_symbol_rules(client, symbol)
                    if rules and "lvr" in rules and rules["lvr"]:
                        raw_lvr = float(rules["lvr"])
                        if raw_lvr < 10:
                            leverage = 10.0
                        else:
                            leverage = min(raw_lvr, 20.0)
                except Exception:
                    pass

            if leverage > 0 and notional_value > 0:
                margin = notional_value / leverage
                total_margin += margin

            positions_summary[pos_key] = {
                "symbol": symbol,
                "positionAmt": pos_val.get("positionAmt"),
                "unRealizedProfit": unrealized_profit,
                "leverage": leverage
            }
        except Exception:
            pass

    available_balance = 0.0
    try:
        account_info = client.balance()
        for asset in account_info:
            if asset.get("asset") == "USDT":
                available_balance = float(asset.get("availableBalance", 0))
                break
    except Exception:
        pass

    formatted_gainers = [f"{item['symbol']} ({item['change_percent']:+.2f}%)" for item in top_gainers]
    formatted_losers = [f"{item['symbol']} ({item['change_percent']:+.2f}%)" for item in top_losers]

    new_limits["_market_status"] = {
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "baseline_started_at": baseline_time,
        "total_margin_balance": round(total_margin, 2),
        "available_balance": round(available_balance, 2),
        "top_gainers": formatted_gainers,
        "top_losers": formatted_losers
    }
    
    new_limits["_open_positions_pnl"] = positions_summary
    
    for key, val in limits.items():
        if key not in ["_market_status", "_open_positions_pnl"]:
            new_limits[key] = val
            
    write_position_limits(new_limits)

def process_single_symbol(symbol, client, top_gainers_symbols, top_losers_symbols, position_limits, all_open_positions):
    try:
        print(f"🔍 [THREAD START] Шалгаж эхэллээ: {symbol}")

        data = get_all_indicator_data_from_railway(symbol)
        if not data:
            print(f"⚠️ [{symbol}] - Railway серверээс дата олдсонгүй")
            return

        macd_data = data.get("macd", {}) or {}
        ohlc_data = data.get("ohlc", {}) or {}
        tops_data = data.get("tops", {}) or {}

        macd_up = macd_data.get("macd_up")
        macd_down = macd_data.get("macd_down")
        
        try:
            macd_line1 = float(macd_data.get("line", {}).get("-1", 0))
            macd_line2 = float(macd_data.get("line", {}).get("-2", 0))
        except (TypeError, ValueError):
            macd_line1, macd_line2 = 0.0, 0.0

        max_open = ohlc_data.get("max_open")
        min_open = ohlc_data.get("min_open")
        macd_lineup_limit = macd_data.get("macd_lineup_limit")
        macd_linedown_limit = macd_data.get("macd_linedown_limit")
        
        candles = ohlc_data.get("candles", {})
        try:
            open0 = float(candles.get("0", {}).get("open", 0))
            open1 = float(candles.get("-1", {}).get("open", 0))
        except (TypeError, ValueError):
            open0, open1 = 0.0, 0.0

        openup_limit = ohlc_data.get("openup_limit")
        opendown_limit = ohlc_data.get("opendown_limit")

        update_macd_info(symbol, macd_lineup_limit, macd_linedown_limit, macd_line1, macd_line2)

        long_opened = f"{symbol}_LONG" in all_open_positions
        short_opened = f"{symbol}_SHORT" in all_open_positions

        rules = get_symbol_rules(client, symbol)
        if not rules:
            return

        is_gainer = symbol in top_gainers_symbols
        is_loser = symbol in top_losers_symbols

        def check_and_reset_baseline(condition_name):
            try:
                baselines = read_baseline_file()
                new_macd_init_price = float(tops_data.get("initial_price", 0))
                if new_macd_init_price <= 0:
                    new_macd_init_price = float(macd_data.get("macd_initial_price", 0))
                
                if new_macd_init_price > 0:
                    baselines[symbol] = new_macd_init_price
                    write_baseline_file(baselines)
                    print(f"🔄 [BASELINE RESET] {symbol} койн дээр '{condition_name}' нөхцөл биелж MACD initial price шинэчиллээ: {new_macd_init_price}")
            except Exception as e:
                print(f"🔥 Baseline reset error for {symbol}: {e}")

        if is_gainer:
            if long_opened and open0 < open1 and open0 < openup_limit:
                check_and_reset_baseline("Gainer Long Close Limit")
                pos_info = all_open_positions.get(f"{symbol}_LONG")
                if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                    result = close_long_position(client, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)
                    if result and not result.get("error"):
                        update_limit(symbol, "LONG", None)

            if not long_opened and macd_up and open0 > open1:
                check_and_reset_baseline("Gainer Long Open Limit")
                result = open_long_position(client, symbol, info=rules)
                if result and not result.get("error"):
                    if min_open is not None:
                        update_limit(symbol, "LONG", min_open)

        elif is_loser:
            if short_opened and open0 > open1 and open0 > opendown_limit:
                check_and_reset_baseline("Loser Short Close Limit")
                pos_info = all_open_positions.get(f"{symbol}_SHORT")
                if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                    result = close_short_position(client, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)
                    if result and not result.get("error"):
                        update_limit(symbol, "SHORT", None)

            if not short_opened and macd_down and open0 < open1:
                check_and_reset_baseline("Loser Short Open Limit")
                result = open_short_position(client, symbol, info=rules)
                if result and not result.get("error"):
                    if max_open is not None:
                        update_limit(symbol, "SHORT", max_open)

        else:
            if long_opened and not is_gainer and open0 < open1 and open0 < openup_limit:
                check_and_reset_baseline("Out-of-list Long Close Limit")
                pos_info = all_open_positions.get(f"{symbol}_LONG")
                if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                    result = close_long_position(client, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)
                    if result and not result.get("error"):
                        update_limit(symbol, "LONG", None)

            if short_opened and not is_loser and open0 > open1 and open0 > opendown_limit:
                check_and_reset_baseline("Out-of-list Short Close Limit")
                pos_info = all_open_positions.get(f"{symbol}_SHORT")
                if pos_info and abs(float(pos_info.get('positionAmt', 0))) > 0:
                    result = close_short_position(client, symbol, abs(float(pos_info.get('positionAmt', 0))), info=rules)
                    if result and not result.get("error"):
                        update_limit(symbol, "SHORT", None)

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    client = get_client()
    if not client:
        exit()

    baseline_time = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ Railway API-аар ажиллах бот эхэллээ. Цаг: {baseline_time}")

    executor = ThreadPoolExecutor(max_workers=5)

    while True:
        try:
            all_open_positions = get_open_positions(client)

            if not all_open_positions:
                with open(LIMITS_FILE, "w", encoding="utf-8") as f:
                    json.dump({}, f, indent=2, ensure_ascii=False)
                print(f"✅ Позицийн файл цэвэрлэгдлээ. Цаг: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
            top_gainers_raw, top_losers_raw = get_railway_top_movers()
            
            update_market_and_pnl_status(client, top_gainers_raw, top_losers_raw, all_open_positions, baseline_time)
            
            if not top_gainers_raw and not top_losers_raw:
                print("DEBUG -> Жагсаалт хоосон байна...")
                time.sleep(2)
                continue

            active_gainers = [item["symbol"] for item in top_gainers_raw[:ACTIVE_TOP_N]]
            active_losers = [item["symbol"] for item in top_losers_raw[:ACTIVE_TOP_N]]

            position_symbols = {
                pos["symbol"]
                for pos in all_open_positions.values()
            }

            symbols_to_check = sorted(
                set(active_gainers) | set(active_losers) | position_symbols
            )
            
            print(f"DEBUG -> Шалгах коруудын жагсаалт: {symbols_to_check}")

            position_limits = read_position_limits()

            future_to_symbol = {
                executor.submit(
                    process_single_symbol,
                    symbol,
                    client,
                    active_gainers,    
                    active_losers,    
                    position_limits,
                    all_open_positions
                ): symbol
                for symbol in symbols_to_check
            }
            
            for future in as_completed(future_to_symbol):
                try:
                    future.result()
                except Exception as exc:
                    pass

            print("✅ Бүх корууд шалгагдлаа. 5 секунд хүлээнэ...\n")
            time.sleep(5)

        except KeyboardInterrupt:
            print("\n🛑 Програмыг зогсоолоо.")
            executor.shutdown(wait=False)
            break

        except Exception as e:
            traceback.print_exc()
            time.sleep(10)
