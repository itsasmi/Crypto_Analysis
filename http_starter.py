import azure.functions as func
import azure.durable_functions as df
import logging
import json

bp = func.Blueprint()

@bp.route(route="start-binance-incremental", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_starter(req: func.HttpRequest, client: df.DurableOrchestrationClient ) -> func.HttpResponse:
    """
    Manual HTTP-triggered execution for on-demand processing of specific trading pairs.
    Request body: {"trading_pair": "BTCUSDT"}
    """
    try:
        req_body = req.get_json()
        trading_pair = req_body.get('trading_pair')
    except (ValueError, AttributeError):
        return func.HttpResponse("Invalid JSON body. Please provide a body like {'trading_pair': 'BTCUSDT'}", status_code=400)

    if not trading_pair:
        return func.HttpResponse("Please pass a 'trading_pair' in the request body.", status_code=400)

    instance_id = f"manual-{trading_pair}"
    existing_instance = await client.get_status(instance_id)
    
    # --- CORRECTED LOGIC ---
    # This check prevents starting a new instance only if the existing one is actively running.
    # It allows you to re-run an orchestration that has completed, failed, or been terminated.
    if existing_instance and existing_instance.runtime_status not in [
        df.OrchestrationRuntimeStatus.Completed,
        df.OrchestrationRuntimeStatus.Failed,
        df.OrchestrationRuntimeStatus.Terminated,
    ]:
        return func.HttpResponse(
            f"An orchestration for {trading_pair} is already in a non-terminal state: {existing_instance.runtime_status} (ID: {instance_id}).",
            status_code=409 # 409 Conflict is an appropriate HTTP status code
        )
    # --- END CORRECTED LOGIC ---

    # Start the new orchestration. This will overwrite the history of the previous completed/failed instance.
    logging.info(f"Starting new orchestration with ID = '{instance_id}' for trading pair '{trading_pair}'.")
    instance_id = await client.start_new("hybrid_orchestrator", instance_id, {"trading_pair": trading_pair})

    return client.create_check_status_response(req, instance_id)
