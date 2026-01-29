import azure.functions as func
import azure.durable_functions as df
import logging

# Initialize the App
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS )

# 1. Add a simple health check directly in this file
@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("Function App is Loading Correctly", status_code=200)

# 2. Import and Register Blueprints
try:
    from http_starter import bp as http_starter_bp
    from year_orchestrator import bp as year_orchestrator_bp
    from binance_month_activity import bp as binance_activity_bp
    from log_manager import bp as log_manager_bp
    from tracking_http import bp as tracking_http_bp

    app.register_blueprint(http_starter_bp )
    app.register_blueprint(year_orchestrator_bp)
    app.register_blueprint(binance_activity_bp)
    app.register_blueprint(log_manager_bp)
    app.register_blueprint(tracking_http_bp )
    logging.info("All blueprints registered successfully.")
except Exception as e:
    logging.error(f"Failed to register blueprints: {str(e)}")
