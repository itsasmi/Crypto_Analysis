import azure.functions as func
import logging
import os
import json
from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime, timezone

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

TABLE_NAME = "TrackingTable"

@app.route(route="update_tracking")
def update_tracking(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request to update tracking.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON in request body", status_code=400)

    trading_pair = req_body.get('trading_pair')
    latest_time = req_body.get('latest_time')
    level = req_body.get('level', 'SILVER').upper()
    row_count = req_body.get('row_count', 0)

    if not trading_pair or not latest_time:
        return func.HttpResponse(
            "Please pass trading_pair and latest_time in the request body",
            status_code=400
        )

    connection_string = os.environ.get("AzureWebJobsStorage")
    if not connection_string:
        return func.HttpResponse("Storage connection string not configured", status_code=500)

    try:
        table_client = TableClient.from_connection_string(connection_string, TABLE_NAME)
        
        # Ensure table exists
        try:
            table_client.create_table()
        except Exception:
            pass # Table likely already exists

        entity = {
            "PartitionKey": level,
            "RowKey": trading_pair,
            "Latest_time": latest_time,
            "rowcount": int(row_count),
            "LastUpdated": datetime.now(timezone.utc).isoformat()
        }

        table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=entity)
        
        logging.info(f"[{trading_pair}] Successfully updated {TABLE_NAME} with time {latest_time}")
        
        return func.HttpResponse(
            json.dumps({"status": "success", "message": f"Updated {trading_pair} to {latest_time}"}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error updating table: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
