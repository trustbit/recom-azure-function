import logging
import json
import azure.functions as func
import azure.durable_functions as df

# Create blueprint instance
bp = func.Blueprint()

# --------------- Blob Trigger Function ---------------
@bp.blob_trigger(arg_name="myblob", path="recom-data", connection="AzureWebJobsStorage") 
@bp.durable_client_input(client_name="starter")
async def blob_trigger(myblob: func.InputStream, starter: df.DurableOrchestrationClient):
    """Blob trigger function that starts the orchestrator when a new blob is uploaded"""
    logging.info(f"Python blob trigger function processed blob: {myblob.name}")

    try:
        # Read the blob content as JSON
        blob_content = myblob.read().decode("utf-8")
        config = json.loads(blob_content)

        # Extract parameters from blob
        manufacturer = config.get("manufacturer", "recom")
        product_types = config.get(
            "product_types", ["dc-dc-converters", "ac-dc-power-supplies"]
        )

        # Prepare input for orchestrator
        orchestrator_input = {
            "manufacturer": manufacturer,
            "product_types": product_types,
        }

        # Start the orchestration
        instance_id = await starter.start_new("orchestrator", None, orchestrator_input)

        logging.info(f"Started orchestration with ID: {instance_id}")
        return instance_id

    except Exception as e:
        logging.error(f"Error starting orchestration from blob trigger: {str(e)}")
        raise

# --------------- HTTP Trigger to Start Orchestrator ---------------
@bp.route(route="startOrchestrator", auth_level=func.AuthLevel.FUNCTION)
@bp.durable_client_input(client_name="starter")
async def start_orchestrator(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    """HTTP trigger function that starts the orchestrator"""
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Get request body
        req_body = req.get_json()
        manufacturer = req_body.get("manufacturer", "recom")
        product_types = req_body.get(
            "product_types", ["dc-dc-converters", "ac-dc-power-supplies"]
        )

        # Prepare input for orchestrator
        orchestrator_input = {
            "manufacturer": manufacturer,
            "product_types": product_types,
        }

        # Start the orchestration
        instance_id = await starter.start_new("orchestrator", None, orchestrator_input)

        logging.info(f"Started orchestration with ID: {instance_id}")

        # Return HTTP response with instance ID
        return func.HttpResponse(
            body=json.dumps({"id": instance_id}),
            mimetype="application/json",
            status_code=202
        )
    except Exception as e:
        logging.error(f"Error starting orchestration: {str(e)}")
        return func.HttpResponse(
            body=f"Error: {str(e)}",
            status_code=500
        )