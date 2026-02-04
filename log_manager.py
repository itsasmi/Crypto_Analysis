import azure.functions as func
import azure.durable_functions as df
import logging
import pyodbc
import os
from datetime import datetime, timezone
from dateutil import parser

bp = func.Blueprint()

# Default start date for new trading pairs: January 1st, 2021, 00:00:00 UTC
DEFAULT_START_DATE = "2021-01-01T00:00:00Z"

def get_connection():
    conn_str = os.getenv("SYNAPSE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("SYNAPSE_CONNECTION_STRING environment variable is not set.")
    return pyodbc.connect(conn_str)

@bp.activity_trigger(input_name="tradingPair")
def get_last_timestamp_activity(tradingPair: str) -> str:
    """
    Activity to fetch the last processed timestamp from Synapse tracking table.
    """
    logging.info(f"Activity get_last_timestamp_activity: Fetching timestamp for {tradingPair}")
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT Last_Processed_Timestamp 
                FROM logging.TrackingTable 
                WHERE Table_Name = 'RAW_INGESTION' AND Trading_Pair = ?
                """
                cursor.execute(query, (tradingPair,))
                row = cursor.fetchone()
                
                if row:
                    ts = row[0]
                    logging.info(f"Activity get_last_timestamp_activity: Found timestamp {ts} for {tradingPair}")
                    return ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                else:
                    logging.info(f"Activity get_last_timestamp_activity: No record for {tradingPair}, using default {DEFAULT_START_DATE}")
                    return DEFAULT_START_DATE
    except Exception as e:
        logging.error(f"Activity get_last_timestamp_activity: CRITICAL ERROR for {tradingPair}: {str(e)}")
        # Re-raise the exception to ensure the orchestrator knows the activity failed.
        raise

@bp.activity_trigger(input_name="params")
def update_tracking_activity(params: dict) -> bool:
    """
    Activity to update or insert the tracking record for a trading pair.
    """
    trading_pair = params.get("trading_pair")
    last_processed_timestamp_str = params.get("last_processed_timestamp")
    record_count = params.get("record_count", 0)
    full_refresh = params.get("full_refresh", False)
    
    logging.info(f"Activity update_tracking_activity: Updating {trading_pair} to {last_processed_timestamp_str}")
    
    try:
        last_processed_timestamp = parser.isoparse(last_processed_timestamp_str)
        
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM logging.TrackingTable WHERE Table_Name = 'RAW_INGESTION' AND Trading_Pair = ?", (trading_pair,))
                exists = cursor.fetchone()[0] > 0
                
                system_update_time = datetime.now(timezone.utc)
                
                if exists:
                    query = """
                    UPDATE logging.TrackingTable
                    SET Last_Processed_Timestamp = ?,
                        Record_Count = ?,
                        System_Update_Time = ?,
                        Full_refresh = ?
                    WHERE Table_Name = 'RAW_INGESTION' AND Trading_Pair = ?
                    """
                    cursor.execute(query, (last_processed_timestamp, record_count, system_update_time, 1 if full_refresh else 0, trading_pair))
                else:
                    query = """
                    INSERT INTO logging.TrackingTable (Table_Name, Trading_Pair, Last_Processed_Timestamp, Record_Count, System_Update_Time, Full_refresh)
                    VALUES ('RAW_INGESTION', ?, ?, ?, ?, ?)
                    """
                    cursor.execute(query, (trading_pair, last_processed_timestamp, record_count, system_update_time, 1 if full_refresh else 0))
                
                conn.commit()
                logging.info(f"Activity update_tracking_activity: Successfully updated tracking table for {trading_pair} to {last_processed_timestamp}")
                return True
    except Exception as e:
        logging.error(f"Activity update_tracking_activity: CRITICAL ERROR updating tracking table for {trading_pair}: {str(e)}")
        # Re-raise the exception.
        raise
