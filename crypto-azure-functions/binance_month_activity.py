# binance_month_activity.py
	
import azure.durable_functions as df
import logging
import os
import requests
import csv
import io
import json
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
	
bp = df.Blueprint()
	
BINANCE_BASE_URL = "https://api.binance.com"
	
bp = df.Blueprint()

@bp.activity_trigger(input_name="activityInput")
def binance_month_activity(activityInput: dict):
    # STRICTLY use local variables from input to prevent cross-coin leakage
    trading_pair = activityInput["trading_pair"]
    year = int(activityInput["year"])
    month = int(activityInput["month"])
    
    logging.info(f"[{trading_pair}] FETCHING: {year}-{month}")
    
    try:
        start_of_month = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_of_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_of_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        start_time_ms = int(start_of_month.timestamp() * 1000)
        end_time_ms = int(end_of_month.timestamp() * 1000) - 1

        all_klines = []
        current_start = start_time_ms

        while current_start < end_time_ms:
            params = {
                "symbol": trading_pair,
                "interval": "1m",
                "startTime": current_start,
                "endTime": end_time_ms,
                "limit": 1000
            }           
            response = requests.get("https://api.binance.com/api/v3/klines", params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not data: break
            all_klines.extend(data)
            current_start = data[-1][6] + 1

        if not all_klines:
            return {"status": "success", "records": 0, "latest_time": end_of_month.isoformat()}

        latest_ts_ms = all_klines[-1][6]
        latest_time_iso = datetime.fromtimestamp(latest_ts_ms / 1000, tz=timezone.utc).isoformat()

        # CSV Upload
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(["trading_pair", "open_time", "open_price", "high_price", "low_price", "close_price", "volume", "close_time", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"])
        for kline in all_klines:
            writer.writerow([trading_pair] + kline[:11])
        
        account_name = os.environ["STORAGE_ACCOUNT_NAME"]
        account_key = os.environ["STORAGE_ACCOUNT_KEY"]
        blob_service = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
        container_client = blob_service.get_container_client("raw")
        
        # DYNAMIC PATH: Ensures isolation
        blob_path = f"binance/{trading_pair}/{year}/{month:02d}.csv"
        container_client.upload_blob(name=blob_path, data=csv_buffer.getvalue(), overwrite=True)

        return {
            "trading_pair": trading_pair,
            "year": year,
            "month": month,
            "records": len(all_klines),
            "latest_time": latest_time_iso,
            "status": "success"
        }
    except Exception as e:
        logging.error(f"[{trading_pair}] ERROR: {str(e)}")
        return {"status": "error", "message": str(e)}