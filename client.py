import os
import threading
import traceback
from binance.um_futures import UMFutures
from binance.error import ClientError
import math
import json
import time

# --- ХАСАХ: Эдгээр импортууд одоо хэрэггүй болсон ---
# from aaa_min import get_trade_rules as get_min_rules
# from aaa_lvr import get_max_leverage

def get_client(api_key=None, api_secret=None):
    """
    Binance API client үүсгэх.
    Хэрэв түлхүүрүүд өгөгдөөгүй бол .env файлаас уншихыг оролдоно.
    """
    if not api_key:
        api_key = os.getenv("BINANCE_API_KEY")
    if not api_secret:
        api_secret = os.getenv("BINANCE_API_SECRET")
    
    if not api_key or not api_secret:
        print("🔥 CRITICAL ERROR: Missing Binance API keys")
        return None

    # --- ЗАСВАР: `requests_params` нь constructor-ийн параметр биш ---
    # Client-г үүсгэсний дараа session-д нь тохиргоог хийж өгнө.
    client = UMFutures(key=api_key, secret=api_secret)
    
    # API дуудлага бүрт 10 секундын timeout тохируулах.
    # Програм сүлжээнээс болж гацахаас сэргийлнэ.
    client.session.requests_params = {"timeout": 10}

    return client

# --- ЗАСВАР: Глобал client-ийн оронд функцүүд client-г параметрээр авна ---
# client = get_client()

# --- ШИНЭ: Symbol-ийн дүрмийг хадгалах кэш ---
symbol_rules_cache = {}
cache_lock = threading.Lock()
cache_initialized = threading.Event() # Кэш хийгдэж дууссан эсэхийг мэдэгдэх event

# --- ШИНЭ: Кэш хийх thread-г нэг л удаа ажиллуулах ---
cache_thread_started = False

# Сүүлд ордер хийсэн цагийг хадгалах dictionary
last_order_time = {}

def safe_order(client, symbol, side, positionSide, order_type, qty, cooldown_seconds=5, info=None):
    """Ордерыг алдаанаас хамгаалж, cooldown-тай илгээх нэгдсэн функц."""
    if not client:
        msg = f"🔥 ORDER BLOCKED: No API client available for {symbol}"
        print(msg)
        return {"error": msg}
    if qty <= 0:
        msg = f"🔥 ORDER BLOCKED: Non-positive qty for {symbol}: {qty}"
        print(msg)
        return {"error": msg}
    now = time.time()
    if now - last_order_time.get(symbol, 0) < cooldown_seconds:
        msg = f"🔁 ORDER COOLDOWN: Skipping order for {symbol} (last {now - last_order_time.get(symbol,0):.1f}s ago)"
        print(msg)
        return {"error": msg}

    # ЗАСВАР: info-г функцийн параметрээр шууд авна, get_symbol_info-г дуудахгүй.
    if not info:
        msg = f"🔥 ORDER BLOCKED: Symbol info (lvr, step_size) was not provided for {symbol}."
        print(msg)
        return {"error": msg}

    # --- ЗАСВАР: Notional шалгалтыг тоймлосны дараа хийх ---
    # Эхлээд qty-г тоймлоно.
    rounded_qty = round_to_step(qty, info['step_size'])
    if rounded_qty <= 0:
        msg = f"🔥 WARNING: Quantity for {symbol} became 0 after rounding. Original: {qty}, step: {info.get('step_size')}"
        print(msg)
        return {"error": msg}

    # Дараа нь тоймлосон qty-г ашиглан notional үнийн дүнг тооцоолно.
    current_price = info.get('current_price', 0) # get_symbol_rules-аас үнийг авна
    notional_value = rounded_qty * current_price

    # --- ШИНЭЧИЛСЭН ХӨШҮҮРГИЙН ЛОГИК ---
    # 1. Хөшүүргийг market_buren.py-аас ирсэн мэдээллээс авах
    target_leverage = info.get('lvr')
    if target_leverage is None:
        msg = f"🔥 ORDER BLOCKED: Leverage info not found for {symbol} in symbol data."
        print(msg)
        return {"error": msg}
    
    # 2. Хөшүүргийн дүрмийг хэрэгжүүлэх (10-аас багаг блоклох, 20-оос ихийг 20 болгох)
    if target_leverage < 10:
        msg = f"🔥 ORDER BLOCKED: Leverage for {symbol} is {target_leverage}x, which is less than 10x."
        print(msg)
        return {"error": msg}
    
    target_leverage = min(target_leverage, 20) # 20-оос их байвал 20 болгох

    try:
        print(f"ORDER: {symbol} | {side} {positionSide} | Qty: {rounded_qty} | Lvr: {target_leverage}x")
        # --- ЗАСВАР: Хөшүүргийг ордер явуулахын өмнө тохируулах ---
        change_leverage(client, symbol, target_leverage)

        result = client.new_order(symbol=symbol, side=side, positionSide=positionSide, type=order_type, quantity=rounded_qty)
        last_order_time[symbol] = now # Цагийг шинэчлэх
        return result
    except ClientError as e:
            # -2022: ReduceOnly Order is rejected. This means the position is already closed.
            if e.error_code == -2022:
                print(f"✅ {symbol}: Position likely already closed. Returning error for state sync.")
                return {"error_code": e.error_code, "error_message": e.error_message}

            # Бусад төрлийн ClientError-г хэвлээд зогсох
            print(f"🔥 ORDER ERROR: {side} {symbol} ({e.status_code}, {e.error_code}, '{e.error_message}')")
            traceback.print_exc()
            return {"error": f"ClientError: {e.error_message}"}

    except Exception as e:
        print(f"🔥 UNEXPECTED ERROR on {side} {symbol}: {e}")
        traceback.print_exc()
        return {"error": f"Unexpected Error: {e}"}
def open_long_position(client, symbol, info):
    # --- ШИНЭ: Тоо хэмжээг энд тооцоолно ---
    min_coin_qty = info.get('min_coin', 0)
    step_size = info.get('step_size', 0)
    if min_coin_qty <= 0: return None
    # Арилжааны доод хэмжээнээс 20% илүүгээр ордер нээнэ
    qty = round_to_step(min_coin_qty * 1.2, step_size)

    return safe_order(
        client=client, symbol=symbol,
        side="BUY",
        positionSide="LONG",
        order_type="MARKET",
        qty=qty,
        info=info
    )

def open_short_position(client, symbol, info):
    # --- ШИНЭ: Тоо хэмжээг энд тооцоолно ---
    min_coin_qty = info.get('min_coin', 0)
    step_size = info.get('step_size', 0)
    if min_coin_qty <= 0: return None
    # Арилжааны доод хэмжээнээс 20% илүүгээр ордер нээнэ
    qty = round_to_step(min_coin_qty * 1.2, step_size)

    return safe_order(
        client=client, symbol=symbol,
        side="SELL",
        positionSide="SHORT",
        order_type="MARKET",
        qty=qty,
        info=info
    )

def close_long_position(client, symbol, qty, info):
    # ЗАСВАР: Позиц хаах үед үржүүлэхгүй, зөвхөн одоо байгаа хэмжээгээр хаана.
    # Cooldown хэрэггүй тул 0 болгож дамжуулна.
    return safe_order(
        client=client, symbol=symbol,
        side="SELL",
        positionSide="LONG",
        order_type="MARKET",
        qty=qty,
        cooldown_seconds=0,
        info=info
    )

def close_short_position(client, symbol, qty, info):
    # ЗАСВАР: Позиц хаах үед үржүүлэхгүй.
    return safe_order(
        client=client, symbol=symbol,
        side="BUY",
        positionSide="SHORT",
        order_type="MARKET",
        qty=qty,
        cooldown_seconds=0,
        info=info
    )

def get_open_positions(client):
    if not client:
        return {}

    try:
        positions = client.get_position_risk()
        open_positions = {}

        for p in positions:
            if float(p["positionAmt"]) != 0:
                key = f"{p['symbol']}_{p['positionSide']}"
                open_positions[key] = p
        return open_positions
    except Exception as e:
        print(f"🔥 Error fetching open positions: {e}")
        return {}

def round_to_step(qty, step):
    """Тоог өгөгдсөн step_size-д тааруулж тоймлоно."""
    if step is None or step <= 0:
        return qty
    # step_size-ийн нарийвчлалыг (аравтын орны тоо) олох
    precision = 0
    if "." in str(step):
        precision = len(str(step).split(".")[1].rstrip("0"))
    return round(math.floor(qty / step) * step, precision)

def _fetch_and_cache_all_rules(client):
    """
    Програм ачааллах үед бүх идэвхтэй зоосны дүрмийг нэг удаа татаж, кэшлэх функц.
    Энэ нь exchange_info, leverage_brackets гэх мэт удаан API дуудлагуудыг цөөлнө.
    """
    print("ℹ️ Initializing and caching all symbol rules...")
    try:
        global cache_thread_started
        # --- ЗАСВАР: Timeout-г түр хугацаанд 30 секунд болгож нэмэгдүүлэх ---
        original_timeout = client.session.requests_params.get("timeout", 10)
        client.session.requests_params["timeout"] = 30
        
        print("  - Fetching exchange info...")
        exchange_info = client.exchange_info()
        print("  - Fetching leverage brackets...")
        all_leverage_brackets = client.leverage_brackets()

        # --- ЗАСВАР: Timeout-г буцаагаад хуучин хэвэнд нь оруулах ---
        client.session.requests_params["timeout"] = original_timeout
        print("  - Timeout reset to original value.")

        symbols_info = {s['symbol']: s for s in exchange_info['symbols']}
        leverage_info = {item['symbol']: item for item in all_leverage_brackets}

        # --- ЗАСВАР: aaa_precision-оос хамааралгүй болгож, active.json-г шууд унших ---
        active_coins = []
        try:
            with open("active.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                active_coins = data.get("active", [])
        except (FileNotFoundError, json.JSONDecodeError):
            print("🔥 CRITICAL: active.json not found or is invalid. Cannot cache rules.")
            return # ЗАСВАР: Алдаатай үед функцийг зогсооно

        for symbol in active_coins:
            if symbol not in symbols_info:
                continue

            symbol_info = symbols_info[symbol]
            filters = {f['filterType']: f for f in symbol_info.get('filters', [])}
            lot_size_filter = filters.get('LOT_SIZE', {})
            
            lvr = 20 # Default leverage
            if symbol in leverage_info and leverage_info[symbol].get('brackets'):
                lvr = max(b['initialLeverage'] for b in leverage_info[symbol]['brackets'])

            all_rules = {
                'lvr': lvr,
                'step_size': float(lot_size_filter.get('stepSize', 0)),
                'min_qty': float(lot_size_filter.get('minQty', 0)),
                'min_notional': float(filters.get('MIN_NOTIONAL', {}).get('notional', 5.0))
            }
            with cache_lock:
                symbol_rules_cache[symbol] = all_rules
        print(f"✅ Successfully cached rules for {len(symbol_rules_cache)} active symbols.")
    except Exception as e:
        print(f"🔥 CRITICAL: Failed to initialize symbol rules cache. The program might be stuck. Error: {e}")
        if 'original_timeout' in locals():
            client.session.requests_params["timeout"] = original_timeout
            print("  - Timeout reset to original value after error.")
        traceback.print_exc()
    finally:
        # --- ШИНЭ: Кэш хийж дууссан эсвэл алдаа гарсан ч event-г set хийнэ ---
        cache_initialized.set()
        print("✅ Cache initialization process finished.")

def get_symbol_rules(client, symbol):
    """
    Binance-ээс тухайн зоосны арилжааны дүрмийг (lvr, step_size, min_qty, min_notional)
    татаж, кэшлээд буцаах функц.
    """
    with cache_lock:
        global cache_thread_started
        # --- ШИНЭ: Кэшлэх thread-г зөвхөн нэг удаа эхлүүлэх ---
        if not cache_thread_started:
            print("🚦 Starting background cache initialization...")
            cache_thread = threading.Thread(target=_fetch_and_cache_all_rules, args=(client,), daemon=True)
            cache_thread.start()
            cache_thread_started = True

    # --- ШИНЭ: Кэш хийгдэж дуустал хүлээх (дээд тал нь 60 секунд) ---
    cache_ready = cache_initialized.wait(timeout=60)
    if not cache_ready:
        print("🔥 CRITICAL: Cache initialization timed out after 60 seconds. The bot cannot proceed.")
        return None

    # --- ЗАСВАР: active.json-оос хамааралгүй болгох ---
    # 1. Эхлээд кэшээс хайх
    static_rules = symbol_rules_cache.get(symbol)

    # 2. Хэрэв кэш дотор байхгүй бол API-аас шууд татаж, кэшид нэмэх
    if not static_rules:
        print(f"ℹ️ Rules for {symbol} not in cache. Fetching directly from API...")
        try:
            # --- ЗАСВАР: exchange_info() нь 'symbol' гэсэн аргумент авдаггүй. ---
            # Бүх мэдээллийг татаж, дотроос нь тухайн зоосыг хайж олно.
            all_exchange_info = client.exchange_info()
            symbol_info = next((s for s in all_exchange_info['symbols'] if s['symbol'] == symbol), None)
            if not symbol_info:
                print(f"🔥 Could not find exchange info for {symbol} in API response.")
                return None

            leverage_brackets = client.leverage_brackets(symbol=symbol)

            filters = {f['filterType']: f for f in symbol_info.get('filters', [])}
            lot_size_filter = filters.get('LOT_SIZE', {})

            lvr = 20 # Default leverage
            if leverage_brackets and leverage_brackets[0].get('brackets'):
                lvr = max(b['initialLeverage'] for b in leverage_brackets[0]['brackets'])

            new_rules = {
                'lvr': lvr,
                'step_size': float(lot_size_filter.get('stepSize', 0)),
                'min_qty': float(lot_size_filter.get('minQty', 0)),
                'min_notional': float(filters.get('MIN_NOTIONAL', {}).get('notional', 5.0))
            }
            # Кэшид нэмэх
            with cache_lock:
                symbol_rules_cache[symbol] = new_rules
            static_rules = new_rules
            print(f"✅ Successfully fetched and cached rules for {symbol}.")
        except Exception as e:
            print(f"🔥 Failed to fetch rules for new symbol {symbol}: {e}")
            return None
    # --- ЗАСВАР: Дүрэмд одоогийн үнэ болон тооцоолсон min_coin-г нэмэх ---
    try:
        current_price = float(client.ticker_price(symbol=symbol)['price'])
        
        # min_coin-г одоогийн үнэ болон min_notional-д үндэслэн тооцоолох
        min_coin_qty = static_rules['min_qty']
        min_notional_val = static_rules['min_notional']
        step_size = static_rules['step_size']

        effective_min_coin = min_coin_qty
        if current_price > 0 and min_notional_val > 0:
            qty_for_notional = min_notional_val / current_price
            if step_size > 0:
                qty_for_notional = math.ceil(qty_for_notional / step_size) * step_size
            effective_min_coin = max(min_coin_qty, qty_for_notional)

        # Эцсийн үр дүнг нэгтгэх
        final_rules = static_rules.copy()
        final_rules['current_price'] = current_price
        final_rules['min_coin'] = effective_min_coin
        final_rules['min_usdt'] = effective_min_coin * current_price
        
        return final_rules

    except Exception as e:
        print(f"🔥 RULES ERROR: Could not fetch price for {symbol}: {e}")
        return None

def get_order_status(client, symbol, order_id):
    if not client:
        return {}

    try:
        return client.query_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        print(f"🔥 Error getting order status for {symbol} (ID: {order_id}): {e}")
        return {}

def change_leverage(client, symbol, leverage):
    """
    Хөшүүргийг өөрчлөх. Хэрэв аль хэдийн зөв байвал юу ч хийхгүй.
    """
    try:
        if client:
            positions = client.get_position_risk(symbol=symbol)
            if positions:
                # Хэрэв нээлттэй позиц байхгүй бол 'leverage' key байхгүй байж болно.
                # Энэ тохиолдолд шууд leverage-г тохируулахыг оролдоно.
                current_leverage_str = positions[0].get('leverage')
                if current_leverage_str and int(current_leverage_str) == leverage:
                    return True # Хөшүүрэг аль хэдийн зөв байвал юу ч хийхгүй.
            client.change_leverage(symbol=symbol, leverage=leverage)
            return True
    except Exception as e:
        print(f"🔥 Failed to change leverage for {symbol}: {e}")
        return False
