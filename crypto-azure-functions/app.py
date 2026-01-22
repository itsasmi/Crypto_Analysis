import requests
import json
from datetime import datetime, timezone

# -------- CONFIG --------
BASE_URL = "https://api.binance.com"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"

# 2026-01-19 full day (UTC)
START_TIME = int(datetime(2026, 1, 19, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TIME = int(datetime(2026, 1, 19, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

LIMIT = 10
# ------------------------

url = f"{BASE_URL}/api/v3/klines"

params = {
    "symbol": SYMBOL,
    "interval": INTERVAL,
    "startTime": START_TIME,
    "endTime": END_TIME,
    "limit": LIMIT
}

print("Calling Binance API...")
response = requests.get(url, params=params)

print("Status Code:", response.status_code)

# Raw response from Binance
raw_data = response.json()
print(response)
print("\n--- RAW BINANCE RESPONSE ---")
print(json.dumps(raw_data, indent=2))
