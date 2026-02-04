import azure.functions as func
import azure.durable_functions as df
import logging
import requests
import pandas as pd
import os
import io
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from dateutil import parser
from dateutil.relativedelta import relativedelta # Import relativedelta

bp = func.Blueprint()

@bp.activity_trigger(input_name="params")
def process_binance_month_activity(params: dict) -> dict:
    """
    Fetch data from Binance API and save to ADLS.
    Handles regeneration by deleting existing files.
    """
    trading_pair = params.get("trading_pair")
    # This timestamp tells us which month to process.
    start_timestamp_str = params.get("start_timestamp")
    is_regeneration = params.get("is_regeneration", False)
    
    logging.info(f"Activity process_binance_month_activity: Starting for {trading_pair} with reference timestamp {start_timestamp_str}")

    try:
        # --- LOGIC CHANGE ---
        # The timestamp from the orchestrator now simply indicates the month to be processed.
        ref_dt = parser.isoparse(start_timestamp_str)

        # For a normal monthly run, we process the entire month indicated by ref_dt.
        # For regeneration, we process from the start of the month up to the current time.
        start_of_month = ref_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if is_regeneration:
            # For regeneration, the end time is now.
            end_of_period = datetime.now(timezone.utc)
        else:
            # For a historical month, the end time is the start of the *next* month.
            end_of_period = start_of_month + relativedelta(months=1)

        logging.info(f"Activity process_binance_month_activity: Calculated processing window for {trading_pair}: {start_of_month.isoformat()} to {end_of_period.isoformat()}")
        # --- END LOGIC CHANGE ---

        # Blob Storage Configuration
        storage_account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
        storage_account_key = os.environ.get("STORAGE_ACCOUNT_KEY")
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string )
        container_name = "raw"
        
        blob_path = f"binance/{trading_pair}/{start_of_month.year}/{start_of_month.month:02d}.csv"
        
        if is_regeneration:
            logging.info(f"Activity process_binance_month_activity: Regeneration mode for {trading_pair}. Deleting existing blob at {blob_path}")
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
                if blob_client.exists():
                    blob_client.delete_blob()
                    logging.info(f"Activity process_binance_month_activity: Successfully deleted {blob_path}")
            except Exception as e:
                logging.warning(f"Activity process_binance_month_activity: Failed to delete blob {blob_path}: {str(e)}")

        # Fetch data from Binance
        all_klines = []
        # Use the calculated start and end times
        current_start = int(start_of_month.timestamp() * 1000)
        end_ms = int(end_of_period.timestamp() * 1000)
        
        logging.info(f"Activity process_binance_month_activity: Fetching data from Binance for {trading_pair}")
        
        while current_start < end_ms:
            url = "https://api.binance.com/api/v3/klines"
            params_api = {
                "symbol": trading_pair,
                "interval": "1m",
                "startTime": current_start,
                "endTime": end_ms, # Use end_ms, which is the start of the next month
                "limit": 1000
            }
            
            response = requests.get(url, params=params_api )
            if response.status_code != 200:
                logging.error(f"Activity process_binance_month_activity: Binance API error for {trading_pair}: {response.text}")
                raise Exception(f"Binance API error: {response.status_code} - {response.text}")
                
            klines = response.json()
            if not klines:
                break
                
            all_klines.extend(klines)
            current_start = klines[-1][6] + 1
            
            if len(klines) < 1000:
                break

        if not all_klines:
            logging.warning(f"Activity process_binance_month_activity: No new data found for {trading_pair} in the month of {start_of_month.year}-{start_of_month.month:02d}")
            # We still return a success so the orchestrator can proceed to the next month
            return {"record_count": 0}

        # Process into DataFrame
        columns = [
            'open_time', 'open_price', 'high_price', 'low_price', 'close_price', 
            'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ]
        df_data = pd.DataFrame(all_klines, columns=columns)
        df_data['trading_pair'] = trading_pair
        
        final_cols = [
            'trading_pair', 'open_time', 'open_price', 'high_price', 'low_price', 
            'close_price', 'volume', 'close_time', 'quote_asset_volume', 
            'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
        ]
        df_data = df_data[final_cols]
        
        # Upload to ADLS
        csv_buffer = io.StringIO()
        df_data.to_csv(csv_buffer, index=False)
        
        logging.info(f"Activity process_binance_month_activity: Uploading {len(df_data)} records for {trading_pair} to {blob_path}")
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        
        logging.info(f"Activity process_binance_month_activity: Successfully uploaded {trading_pair} data.")
        
        return {"record_count": len(df_data)}
    except Exception as e:
        logging.error(f"Activity process_binance_month_activity: Error processing {trading_pair}: {str(e)}")
        raise
