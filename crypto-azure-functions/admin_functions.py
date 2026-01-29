# admin_functions.py - NEW FILE
import os
import logging
import azure.functions as func
import azure.durable_functions as df
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError

bp = df.Blueprint()

TABLE_NAME = "TrackingTable"  # Same as above

@bp.route(route="admin/delete-table", methods=["DELETE"])
def delete_tracking_table(req: func.HttpRequest) -> func.HttpResponse:
    """Delete the tracking table completely"""
    try:
        connection_string = os.environ["AzureWebJobsStorage"]
        table_service = TableServiceClient.from_connection_string(connection_string)
        
        table_service.delete_table(TABLE_NAME)
        logging.info(f"✓ Table '{TABLE_NAME}' deleted successfully")
        
        return func.HttpResponse(f"Table '{TABLE_NAME}' deleted. Will be recreated on next run.", status_code=200)
    except ResourceNotFoundError:
        return func.HttpResponse(f"Table '{TABLE_NAME}' does not exist", status_code=404)
    except Exception as e:
        logging.error(f"Error deleting table: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@bp.route(route="admin/terminate-all", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def terminate_all_orchestrations(req: func.HttpRequest, client) -> func.HttpResponse:
    """Terminate ALL running orchestrations"""
    try:
        instances = await client.get_status_all()
        terminated = []
        
        for instance in instances:
            if instance.runtime_status in [
                df.OrchestrationRuntimeStatus.Running,
                df.OrchestrationRuntimeStatus.Pending
            ]:
                await client.terminate(instance.instance_id, "Admin cleanup")
                terminated.append(instance.instance_id)
                logging.info(f"✓ Terminated: {instance.instance_id}")
        
        return func.HttpResponse(
            f"Terminated {len(terminated)} instances:\n" + "\n".join(terminated),
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@bp.route(route="admin/list-instances", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def list_all_instances(req: func.HttpRequest, client) -> func.HttpResponse:
    """List all orchestration instances"""
    try:
        instances = await client.get_status_all()
        
        result = []
        for inst in instances:
            result.append({
                "instance_id": inst.instance_id,
                "status": str(inst.runtime_status),
                "created": str(inst.created_time),
                "last_updated": str(inst.last_updated_time)
            })
        
        import json
        return func.HttpResponse(
            json.dumps(result, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)