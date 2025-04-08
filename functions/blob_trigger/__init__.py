import logging
import json
import os
import azure.functions as func
import azure.durable_functions as df


async def main(myblob: func.InputStream, starter: df.DurableOrchestrationClient):
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
