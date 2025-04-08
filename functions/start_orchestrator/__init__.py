import logging
import json
import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    """HTTP trigger that starts the orchestrator function"""
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        # Get request body
        req_body = req.get_json()
        
        # Get parameters
        manufacturer = req_body.get('manufacturer', 'recom')
        product_types = req_body.get('product_types', ['dc-dc-converters', 'ac-dc-power-supplies'])
        
        # Prepare input for orchestrator
        orchestrator_input = {
            "manufacturer": manufacturer,
            "product_types": product_types
        }
        
        # Start the orchestration
        instance_id = await starter.start_new("orchestrator", None, orchestrator_input)
        
        # Return response with instance ID
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "instance_id": instance_id,
                "manufacturer": manufacturer,
                "product_types": product_types,
                "status_query_uri": starter.create_check_status_response(req, instance_id).headers["Location"]
            }),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error starting orchestration: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )