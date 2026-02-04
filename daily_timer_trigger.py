import azure.functions as func
import azure.durable_functions as df
import logging
import datetime

bp = func.Blueprint()

@bp.timer_trigger(schedule="0 15 9 * * *", arg_name="timer")
@bp.durable_client_input(client_name="client")
async def daily_timer_trigger(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    """
    Automated daily timer trigger at 06:30 AM UTC (12:00 PM IST).
    Processes all configured trading pairs.
    """
    if timer.past_due:
        logging.info("The timer is running late!")

    # --- CORRECTED LINE ---
    # Changed "MATIUSDT" to the correct symbol "MATICUSDT"
    trading_pairs = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT", "MATICUSDT"]
    # --------------------

    instance_id_prefix = "daily-timer"
    
    main_instance_id = f"{instance_id_prefix}-main"
    existing_instance = await client.get_status(main_instance_id)
    
    if existing_instance and existing_instance.runtime_status in [df.OrchestrationRuntimeStatus.Running, df.OrchestrationRuntimeStatus.Pending]:
        logging.warning(f"Previous timer execution {main_instance_id} is still running. Skipping this run.")
        return

    await client.start_new("timer_main_orchestrator", main_instance_id, {"trading_pairs": trading_pairs})
    
    logging.info(f"Started daily timer orchestration with ID = '{main_instance_id}'.")

@bp.orchestration_trigger(context_name="context")
def timer_main_orchestrator(context: df.DurableOrchestrationContext):
    """
    Main orchestrator for timer trigger to handle sequential steps:
    Resume Synapse -> Process all pairs in parallel -> Pause Synapse
    """
    input_data = context.get_input()
    trading_pairs = input_data.get("trading_pairs", [])
    instance_id = context.instance_id

    logging.info(f"Timer Orchestrator {instance_id}: Starting daily process for {len(trading_pairs)} pairs.")

    try:
        # 1. Resume Synapse SQL Pool
        logging.info(f"Timer Orchestrator {instance_id}: Resuming Synapse SQL Pool.")
        resume_status = yield context.call_activity("resume_synapse_activity", None)
        
        if resume_status != "Online":
            logging.error(f"Timer Orchestrator {instance_id}: Synapse resume failed. Status: {resume_status}. Aborting.")
            return f"Failed: Synapse resume failed ({resume_status})"

        # 2. Process all trading pairs
        tasks = []
        for pair in trading_pairs:
            tasks.append(context.call_sub_orchestrator("hybrid_orchestrator", {"trading_pair": pair}))
        
        yield context.task_all(tasks)
        logging.info(f"Timer Orchestrator {instance_id}: All trading pairs processed.")

        # 3. Pause Synapse SQL Pool
        logging.info(f"Timer Orchestrator {instance_id}: Pausing Synapse SQL Pool.")
        pause_status = yield context.call_activity("pause_synapse_activity", None)
        
        logging.info(f"Timer Orchestrator {instance_id}: Daily process completed. Synapse status: {pause_status}")
        return "Success"
    except Exception as e:
        error_message = f"Timer Orchestrator {instance_id}: Main orchestrator failed: {str(e)}"
        logging.error(error_message)
        # Attempt to pause Synapse even if processing fails to avoid leaving it running
        logging.info(f"Timer Orchestrator {instance_id}: Attempting to pause Synapse after failure.")
        yield context.call_activity("pause_synapse_activity", None)
        return error_message
