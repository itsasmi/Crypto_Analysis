import azure.functions as func
import azure.durable_functions as df
import logging

# Import all blueprints from your existing function files
from http_starter import bp as http_starter_bp
from daily_timer_trigger import bp as daily_timer_bp
from hybrid_orchestrator import bp as hybrid_orchestrator_bp
from binance_month_activity import bp as binance_month_activity_bp
from log_manager import bp as log_manager_bp

# --- CORRECTED LINE ---
# Import the blueprint from your synapse_automation.py file
from synapse_automation import bp as synapse_automation_bp
# --------------------

# Initialize the DFApp
app = df.DFApp( )

# Register all of your blueprints
app.register_blueprint(http_starter_bp )
app.register_blueprint(daily_timer_bp)
app.register_blueprint(hybrid_orchestrator_bp)
app.register_blueprint(binance_month_activity_bp)
app.register_blueprint(log_manager_bp)

# --- CORRECTED LINE ---
# Register the Synapse automation blueprint
app.register_blueprint(synapse_automation_bp)
# --------------------

logging.info("All blueprints registered successfully.")
