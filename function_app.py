import azure.functions as func
from preprocessing.blueprint import bp as preprocessing_bp

app = func.FunctionApp()

# Register Blueprints
app.register_functions(preprocessing_bp)
