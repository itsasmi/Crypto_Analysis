import azure.functions as func
import azure.durable_functions as df
import logging
import time
import os
import requests
from azure.identity import DefaultAzureCredential

bp = func.Blueprint()

def get_synapse_management_url():
    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
    resource_group = os.environ.get("AZURE_RESOURCE_GROUP")
    workspace_name = os.environ.get("SYNAPSE_WORKSPACE_NAME")
    sql_pool_name = os.environ.get("SYNAPSE_SQL_POOL_NAME")
    
    if not all([subscription_id, resource_group, workspace_name, sql_pool_name]):
        missing_vars = [var for var, value in {
            "AZURE_SUBSCRIPTION_ID": subscription_id,
            "AZURE_RESOURCE_GROUP": resource_group,
            "SYNAPSE_WORKSPACE_NAME": workspace_name,
            "SYNAPSE_SQL_POOL_NAME": sql_pool_name
        }.items() if not value]
        raise ValueError(f"Missing one or more Synapse environment variables: {', '.join(missing_vars)}")

    return f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Synapse/workspaces/{workspace_name}/sqlPools/{sql_pool_name}?api-version=2021-06-01"

def get_auth_token():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token

@bp.activity_trigger(input_name="dummy")
def resume_synapse_activity(dummy: str) -> str:
    """
    Resume the Synapse Dedicated SQL Pool and poll until Online.
    """
    logging.info("Activity resume_synapse_activity: Starting resume process.")
    url = get_synapse_management_url()
    resume_url = f"{url.split('?')[0]}/resume?api-version=2021-06-01"
    
    try:
        token = get_auth_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        # Check current status first
        response = requests.get(url, headers=headers)
        response.raise_for_status() # raise an exception for HTTP errors
        status = response.json().get("properties", {}).get("status")
        logging.info(f"Activity resume_synapse_activity: Current status is {status}")
        
        if status == "Online":
            logging.info("Activity resume_synapse_activity: Synapse is already Online.")
            return "Online"
            
        if status == "Paused":
            logging.info("Activity resume_synapse_activity: Sending resume request.")
            resume_response = requests.post(resume_url, headers=headers)
            resume_response.raise_for_status()
            
        # Poll for status
        for i in range(20):
            time.sleep(30)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get("properties", {}).get("status")
            logging.info(f"Activity resume_synapse_activity: Polling attempt {i+1}, status: {status}")
            
            if status == "Online":
                logging.info("Activity resume_synapse_activity: Synapse is now Online.")
                return "Online"
        
        logging.error(f"Activity resume_synapse_activity: Synapse did not become Online after polling. Final status: {status}")
        return status
        
    except Exception as e:
        logging.error(f"Activity resume_synapse_activity: Error resuming Synapse: {str(e)}")
        raise

@bp.activity_trigger(input_name="dummy")
def pause_synapse_activity(dummy: str) -> str:
    """
    Pause the Synapse Dedicated SQL Pool and poll until Paused.
    """
    logging.info("Activity pause_synapse_activity: Starting pause process.")
    url = get_synapse_management_url()
    pause_url = f"{url.split('?')[0]}/pause?api-version=2021-06-01"
    
    try:
        token = get_auth_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        # Check current status first
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        status = response.json().get("properties", {}).get("status")
        logging.info(f"Activity pause_synapse_activity: Current status is {status}")
        
        if status == "Paused":
            logging.info("Activity pause_synapse_activity: Synapse is already Paused.")
            return "Paused"
            
        if status == "Online":
            logging.info("Activity pause_synapse_activity: Sending pause request.")
            pause_response = requests.post(pause_url, headers=headers)
            pause_response.raise_for_status()
            
        # Poll for status
        for i in range(20):
            time.sleep(30)
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get("properties", {}).get("status")
            logging.info(f"Activity pause_synapse_activity: Polling attempt {i+1}, status: {status}")
            
            if status == "Paused":
                logging.info("Activity pause_synapse_activity: Synapse is now Paused.")
                return "Paused"
        
        logging.error(f"Activity pause_synapse_activity: Synapse did not become Paused after polling. Final status: {status}")
        return status
        
    except Exception as e:
        logging.error(f"Activity pause_synapse_activity: Error pausing Synapse: {str(e)}")
        raise