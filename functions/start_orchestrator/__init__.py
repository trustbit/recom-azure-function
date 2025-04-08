import logging
import json
import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient


async def main(
    req: func.HttpRequest, starter: df.DurableOrchestrationClient
) -> func.HttpResponse:
    """HTTP trigger that either starts the orchestrator function directly or
    creates a blob that will trigger the orchestrator"""
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Get request body
        req_body = req.get_json()

        # Get parameters
        manufacturer = req_body.get("manufacturer", "recom")
        product_types = req_body.get(
            "product_types", ["dc-dc-converters", "ac-dc-power-supplies"]
        )

        # Get trigger mode from request
        trigger_mode = req_body.get("trigger_mode", "direct")

        # Prepare configuration for orchestrator
        config = {
            "manufacturer": manufacturer,
            "product_types": product_types,
        }

        if trigger_mode == "blob":
            # Create a blob that will trigger the orchestrator
            connection_string = req.environ.get("AzureWebJobsStorage")
            container_name = "power-converter-data"
            blob_name = f"config/trigger-{manufacturer}-{'-'.join(product_types)}.json"

            # Create the blob client
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            container_client = blob_service_client.get_container_client(container_name)

            # Create config/directory if it doesn't exist
            if not container_client.exists():
                container_client.create_container()

            # Upload the configuration as a blob
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(json.dumps(config), overwrite=True)

            return func.HttpResponse(
                json.dumps(
                    {
                        "success": True,
                        "trigger_mode": "blob",
                        "manufacturer": manufacturer,
                        "product_types": product_types,
                        "blob_path": f"{container_name}/{blob_name}",
                    }
                ),
                mimetype="application/json",
            )
        else:
            # Start the orchestration directly
            instance_id = await starter.start_new("orchestrator", None, config)

            # Return response with instance ID
            return func.HttpResponse(
                json.dumps(
                    {
                        "success": True,
                        "trigger_mode": "direct",
                        "instance_id": instance_id,
                        "manufacturer": manufacturer,
                        "product_types": product_types,
                        "status_query_uri": starter.create_check_status_response(
                            req, instance_id
                        ).headers["Location"],
                    }
                ),
                mimetype="application/json",
            )
    except Exception as e:
        logging.error(f"Error starting orchestration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
