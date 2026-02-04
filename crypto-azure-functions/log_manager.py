# get_last_processed_status - FIXED
import os
import logging
from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime, timezone
import azure.durable_functions as df
bp = df.Blueprint()
# ✅ FIXED: Use consistent table name
TABLE_NAME = "TrackingTable"  # Or "LogTable" - pick ONE and use everywhere
@bp.activity_trigger(input_name="inputData")
def get_last_processed_status(inputData: dict):
    trading_pair = inputData["trading_pair"]
    level = inputData.get("level", "BRONZE").upper()   
    connection_string = os.environ["AzureWebJobsStorage"]
    table_client = TableClient.from_connection_string(connection_string, TABLE_NAME)    
    try:
        entity = table_client.get_entity(partition_key=level, row_key=trading_pair)        
        l_time_str = entity.get("latest_time") or entity.get("Latest_time") or entity.get("LatestTime")
       
        if l_time_str:
            dt = datetime.fromisoformat(l_time_str.replace('Z', '+00:00'))
            logging.info(f"[{trading_pair}] ✓ FOUND EXISTING: Last={dt.year}/{dt.month}")
            return {"year": dt.year, "month": dt.month, "exists": True}
           
    except ResourceNotFoundError:
        logging.info(f"[{trading_pair}] ✗ NO RECORD FOUND - Will start from 2021/1")
    except Exception as e:
        logging.error(f"[{trading_pair}] ✗ ERROR reading log: {str(e)}")   
        return {"year": 2020, "month": 12, "exists": False}
@bp.activity_trigger(input_name="dataInput")
def update_last_processed_status(dataInput: dict):
    trading_pair = dataInput["trading_pair"]
    connection_string = os.environ["AzureWebJobsStorage"]
    table_client = TableClient.from_connection_string(connection_string, TABLE_NAME)
    try:
        table_client.create_table()
        logging.info(f"[{trading_pair}] Table '{TABLE_NAME}' created/verified")
    except Exception as e:
        logging.debug(f"Table already exists or error: {e}")
    entity = {
        "PartitionKey": dataInput.get("level", "BRONZE").upper(),
        "RowKey": trading_pair,
        "Latest_time": dataInput["latest_time"],  # Backward compatibility
        "rowcount": int(dataInput.get("row_count", 0)),
        "LastUpdated": datetime.now(timezone.utc).isoformat()
    }
    table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=entity)
    logging.info(f"[{trading_pair}] ✓ LOG UPDATED: {dataInput['latest_time']} in table '{TABLE_NAME}'")
    return True