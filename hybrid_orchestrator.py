import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil import parser

bp = func.Blueprint()

@bp.orchestration_trigger(context_name="context")
def hybrid_orchestrator(context: df.DurableOrchestrationContext):
    """
    Core orchestrator implementing the hybrid monthly data processing strategy.
    """
    input_data = context.get_input()
    trading_pair = input_data.get("trading_pair")
    instance_id = context.instance_id
    
    if not trading_pair:
        logging.error(f"Orchestrator {instance_id}: No trading pair provided.")
        return "Error: No trading pair provided."

    logging.info(f"Orchestrator {instance_id}: Starting hybrid processing for {trading_pair}")

    try:
        # 1. Fetch last processed timestamp from Synapse
        logging.info(f"Orchestrator {instance_id}: Calling get_last_timestamp_activity for {trading_pair}")
        last_processed_str = yield context.call_activity("get_last_timestamp_activity", trading_pair)
        
        logging.info(f"Orchestrator {instance_id}: Received timestamp string: {last_processed_str}")
        last_processed_dt = parser.isoparse(last_processed_str)
        
        current_utc_now = context.current_utc_datetime.replace(tzinfo=timezone.utc)
        
        logging.info(f"Orchestrator {instance_id}: {trading_pair} - Last processed: {last_processed_dt}, Current: {current_utc_now}")

        # 2. Compare timestamps and determine logic
        is_current_month = (last_processed_dt.year == current_utc_now.year and 
                            last_processed_dt.month == current_utc_now.month)

        if is_current_month:
            # CASE: CURRENT MONTH REGENERATION
            logging.info(f"Orchestrator {instance_id}: Logic - Current month regeneration for {trading_pair}")
            
            start_of_month = last_processed_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            params = {
                "trading_pair": trading_pair,
                "start_timestamp": start_of_month.isoformat(),
                "end_timestamp": current_utc_now.isoformat(),
                "is_regeneration": True
            }
            
            logging.info(f"Orchestrator {instance_id}: Calling process_binance_month_activity for regeneration")
            result = yield context.call_activity("process_binance_month_activity", params)
            
            if result and result.get("record_count", 0) >= 0:
                logging.info(f"Orchestrator {instance_id}: Calling update_tracking_activity for {trading_pair}")
                yield context.call_activity("update_tracking_activity", {
                    "trading_pair": trading_pair,
                    "last_processed_timestamp": current_utc_now.isoformat(),
                    "record_count": result["record_count"],
                    "full_refresh": True
                })
                logging.info(f"Orchestrator {instance_id}: Completed current month for {trading_pair}. Terminating.")
                return f"Successfully regenerated current month for {trading_pair}. Records: {result['record_count']}"
            else:
                logging.error(f"Orchestrator {instance_id}: Failed regeneration for {trading_pair} - Result: {result}")
                return f"Failed regeneration for {trading_pair}"

        else:
            # CASE: PREVIOUS MONTH INCREMENTAL
            logging.info(f"Orchestrator {instance_id}: Logic - Previous month incremental for {trading_pair}")
            
            next_month_start = (last_processed_dt + relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            params = {
                "trading_pair": trading_pair,
                "start_timestamp": last_processed_dt.isoformat(),
                "end_timestamp": (next_month_start - relativedelta(seconds=1)).isoformat(),
                "is_regeneration": False
            }
            
            logging.info(f"Orchestrator {instance_id}: Calling process_binance_month_activity for incremental month")
            result = yield context.call_activity("process_binance_month_activity", params)
            
            if result and result.get("record_count", 0) >= 0:
                logging.info(f"Orchestrator {instance_id}: Calling update_tracking_activity for {trading_pair}")
                yield context.call_activity("update_tracking_activity", {
                    "trading_pair": trading_pair,
                    "last_processed_timestamp": next_month_start.isoformat(),
                    "record_count": result["record_count"],
                    "full_refresh": False
                })
                
                logging.info(f"Orchestrator {instance_id}: Continuing to next month for {trading_pair}")
                context.continue_as_new({"trading_pair": trading_pair})
                return f"Processed month {last_processed_dt.year}-{last_processed_dt.month:02d}, continuing..."
            else:
                logging.error(f"Orchestrator {instance_id}: Failed to process month {last_processed_dt.year}-{last_processed_dt.month:02d}")
                return f"Failed to process month {last_processed_dt.year}-{last_processed_dt.month:02d} for {trading_pair}."
                
    except Exception as e:
        error_message = f"Orchestrator {instance_id}: Unhandled exception caught: {str(e)}"
        logging.error(error_message)
        return f"Orchestration Failed: {error_message}"

