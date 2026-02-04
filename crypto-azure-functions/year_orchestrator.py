# incremental_orchestrator.py - FIXED
import azure.durable_functions as df
import logging

bp = df.Blueprint()

@bp.orchestration_trigger(context_name="context")
def incremental_orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input()
    trading_pair = input_data["trading_pair"]
    level = "BRONZE"

    logging.info(f"[{trading_pair}] ═══════════════════════════════════════")
    logging.info(f"[{trading_pair}] ORCHESTRATION STARTED: {context.instance_id}")
    logging.info(f"[{trading_pair}] ═══════════════════════════════════════")

    # 1. Get last processed state
    last_state = yield context.call_activity("get_last_processed_status", {
        "trading_pair": trading_pair, 
        "level": level
    })
    
    curr_year = int(last_state["year"])
    curr_month = int(last_state["month"])
    
    logging.info(f"[{trading_pair}] 📅 CURRENT STATE: {curr_year}/{curr_month:02d}")

    # 2. Calculate next month to process
    if curr_month == 12:
        next_year = curr_year + 1
        next_month = 1
    else:
        next_year = curr_year
        next_month = curr_month + 1

    logging.info(f"[{trading_pair}] ➡️  NEXT TO PROCESS: {next_year}/{next_month:02d}")

    # 3. Check if we're caught up to current time
    now = context.current_utc_datetime
    if next_year > now.year or (next_year == now.year and next_month > now.month):
        logging.info(f"[{trading_pair}] ✅ COMPLETED - UP TO DATE!")
        return f"COMPLETED: {trading_pair} is up-to-date (last: {curr_year}/{curr_month:02d})"

    # 4. Process next month
    logging.info(f"[{trading_pair}] 🔄 PROCESSING: {next_year}/{next_month:02d}...")
    activity_input = {
        "trading_pair": trading_pair, 
        "year": next_year, 
        "month": next_month
    }
    
    result = yield context.call_activity("binance_month_activity", activity_input)

    if result.get("status") == "success":
        logging.info(f"[{trading_pair}] ✓ SUCCESS: Processed {result['records']} records for {next_year}/{next_month:02d}")
        
        # 5. Update log table
        update_data = {
            "trading_pair": trading_pair,
            "level": level,
            "row_count": result["records"],
            "latest_time": result["latest_time"]
        }
        yield context.call_activity("update_last_processed_status", update_data)
        
        # 6. Continue to next month
        logging.info(f"[{trading_pair}] ⏭️  CONTINUING to next month...")
        context.continue_as_new(input_data)
        return f"[{trading_pair}] Processed {next_year}/{next_month:02d}"
    else:
        logging.error(f"[{trading_pair}] ✗ FAILED at {next_year}/{next_month:02d}: {result.get('message')}")
        return f"FAILED: {trading_pair} at {next_year}/{next_month:02d}"