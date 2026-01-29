# http_start_binance_incremental.py - FIXED
import logging
from datetime import datetime, timezone
import azure.functions as func
import azure.durable_functions as df

bp = df.Blueprint()

@bp.route(route="start-binance-incremental", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_start_binance_incremental(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Please provide JSON body with 'trading_pair'", status_code=400)

    trading_pair = body.get("trading_pair")
    if not trading_pair:
        return func.HttpResponse("JSON must contain 'trading_pair'", status_code=400)

    # ✅ FIXED: Single instance per trading pair (prevents duplicates)
    instance_id = f"inc-{trading_pair}"
    
    # Check if already running
    existing = await client.get_status(instance_id)
    if existing and existing.runtime_status in [
        df.OrchestrationRuntimeStatus.Running,
        df.OrchestrationRuntimeStatus.Pending
    ]:
        logging.warning(f"[{trading_pair}] ⚠️  ALREADY RUNNING: {instance_id}")
        return func.HttpResponse(
            f"ERROR: {trading_pair} is already running.\nInstance: {instance_id}\nStatus: {existing.runtime_status}",
            status_code=409
        )
    
    # Start new orchestration
    await client.start_new(
        "incremental_orchestrator",
        instance_id=instance_id,
        client_input={"trading_pair": trading_pair}
    )
    
    logging.info(f"[{trading_pair}] 🚀 STARTED NEW ORCHESTRATION: {instance_id}")
    return client.create_check_status_response(req, instance_id)