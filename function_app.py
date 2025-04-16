import azure.functions as func
import logging

# Import blueprints
from blueprints.triggers import bp as triggers_bp
from blueprints.orchestrator import bp as orchestrator_bp
from blueprints.series_scraper import bp as series_bp
from blueprints.product_scraper import bp as products_bp
from blueprints.pdf_handler import bp as pdf_bp
from blueprints.data_processor import bp as data_bp

# Create the main function app
app = func.FunctionApp()

# Register all blueprint functions
app.register_functions(triggers_bp)
app.register_functions(orchestrator_bp)
app.register_functions(series_bp)
app.register_functions(products_bp)
app.register_functions(pdf_bp)
app.register_functions(data_bp)
