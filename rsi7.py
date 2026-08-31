import time
import requests

# Railway дээрх өөрийнхөө public URL-ийг энд бичнэ (v/s төгсгөлд нь /candles гэж өгнө)
BASE_URL = "https://burger2-production.up.railway.app/candles"

# Хянахыг хүссэн койнуудын жагсаалт (Эсвэл бүх койноор гүйж болно)
TARGET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def calculate_rsi(closes, period=7):
    """Цэвэр Python-оор RSI тооцоолох функц"""
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []
    
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        return 100.0

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)


def get_market_data():
    """Railway серверээс бүх койны датаг татаж авах"""
    try:
        response = requests.get(BASE_URL, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch data from server: {e}")
    return None


def main():
    print("=" * 50)
    print("RSI-7 MONITORING BOT STARTED")
    print("=" * 50)

    while True:
        print(f"\n[INFO] Fetching data and calculating RSI-7 at {time.strftime('%H:%M:%S')}...")
        
        data = get_market_data()
        
        if not data:
            print("[WARNING] No data received, retrying in 10 seconds...")
            time.sleep(10)
            continue

        # Хүссэн койнуудаараа гүйж RSI-г тооцоолно
        for symbol in TARGET_SYMBOLS:
            if symbol in data:
                candles = data[symbol] # [ [open_time, open, high, low, close, volume, close_time], ... ]
                
                # Лааны датануудаас зөвхөн хаалтын үнийг (close price - 4 дэх индекс) салгаж авна
                closes = [candle[4] for candle in candles]
                
                rsi = calculate_rsi(closes, period=7)
                
                if rsi is not None:
                    print(f"-> {symbol} | Current Close: {closes[-1]} | RSI-7: {rsi}")
                else:
                    print(f"-> {symbol} | Not enough candles for RSI")
            else:
                print(f"-> {symbol} | Symbol not found in server data")

        # 10 секунд эсвэл 1 минут хүлээх (Хүссэн хугацаагаар тохируулж болно)
        time.sleep(10)

if __name__ == "__main__":
    main()
