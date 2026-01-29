import json
import logging
import azure.functions as func
import azure.durable_functions as df
from log_manager import update_last_processed_status

bp = df.Blueprint()
@bp.function_name("update_tracking")
@bp.route(
    route="update-tracking",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS  # ⚠️ Match your DFApp setting
)
def update_tracking(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('=== Update Tracking Function Called ===')
    
    try:
        # Get the request body
        body = req.get_json()
        logging.info(f"Received body: {body}")

        # Map ADF fields to log_manager format
        data_input = {
            "level": body.get("stage", "SILVER"),
            "trading_pair": body.get("symbol"),
            "latest_time": body.get("latest_time"),
            "row_count": body.get("row_count", 0)
        }
        
        logging.info(f"Mapped data_input: {data_input}")

        # Call your existing log_manager function
        update_last_processed_status(data_input)

        return func.HttpResponse(
            json.dumps({
                "status": "success", 
                "message": f"Updated {data_input['level']} for {data_input['trading_pair']}"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except ValueError as ve:
        logging.error(f"JSON parsing error: {str(ve)}")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON format"}),
            status_code=400,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"Error in update_tracking: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )