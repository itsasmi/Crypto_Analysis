import requests
from datetime import datetime, timezone, timedelta

BINANCE_BASE_URL = "https://api.binance.com/api/v3/klines"


def fetch_month_klines(symbol: str, year: int, month: int):
    """
    Fetch full 1-minute Binance klines for a given month (UTC)
    Returns raw array-of-arrays (Binance native format)
    """

    start_of_month = datetime(year, month, 1, tzinfo=timezone.utc)

    if month == 12:
        end_of_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_of_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    start_time = int(start_of_month.timestamp() * 1000)
    end_time = int(end_of_month.timestamp() * 1000)

    all_klines = []

    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_time,
            "limit": 1000
        }

        response = requests.get(BINANCE_BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_klines.extend(data)

        # move cursor forward using last close time
        start_time = data[-1][6] + 1

    return all_klines
