import azure.functions as func
import logging
import os
import requests
import csv
import io
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

BINANCE_BASE_URL = "https://api.binance.com"

@app.route(route="binance_month_fetch", auth_level=func.AuthLevel.ANONYMOUS)
def binance_month_fetch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Binance monthly CSV fetch started")

    # -------- Parameters --------
    symbol = req.params.get("symbol", "BTCUSDT")
    year = int(req.params.get("year"))
    month = int(req.params.get("month"))
    interval = "1m"

    # -------- Time window --------
    start_of_month = datetime(year, month, 1, tzinfo=timezone.utc)

    if month == 12:
        end_of_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_of_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    start_time = int(start_of_month.timestamp() * 1000)
    end_time = int(end_of_month.timestamp() * 1000)

    all_klines = []

    # -------- Fetch Binance data (IN MEMORY) --------
    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "limit": 1000
        }

        response = requests.get(
            f"{BINANCE_BASE_URL}/api/v3/klines",
            params=params,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        all_klines.extend(data)
        start_time = data[-1][6] + 1  # closeTime + 1 ms

    logging.info(f"Fetched {len(all_klines)} klines")

    # -------- Build CSV in memory --------
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # ---- HEADER (ONLY ONCE) ----
    writer.writerow([
        "symbol",
        "interval",
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume"
    ])

    # ---- DATA ROWS ----
    for kline in all_klines:
        writer.writerow([
            symbol,
            interval,
            kline[0],   # open_time
            kline[1],   # open_price
            kline[2],   # high_price
            kline[3],   # low_price
            kline[4],   # close_price
            kline[5],   # volume
            kline[6],   # close_time
            kline[7],   # quote_asset_volume
            kline[8],   # number_of_trades
            kline[9],   # taker_buy_base_asset_volume
            kline[10]   # taker_buy_quote_asset_volume
        ])

    csv_buffer.seek(0)

    # -------- Storage (SINGLE WRITE) --------
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["STORAGE_ACCOUNT_KEY"]
    container_name = "raw"

    blob_service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )

    container_client = blob_service.get_container_client(container_name)

    blob_path = f"binance/{symbol}/{year}/{month:02d}.csv"

    container_client.upload_blob(
        name=blob_path,
        data=csv_buffer.getvalue(),
        overwrite=True
    )

    logging.info(f"CSV written to {blob_path}")

    return func.HttpResponse(
        f"Stored {len(all_klines)} records → {blob_path}",
        status_code=200
    )
