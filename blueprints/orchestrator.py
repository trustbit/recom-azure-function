import azure.functions as func
import azure.durable_functions as df

# Create blueprint instance
bp = func.Blueprint()

# --------------- Orchestrator Function ---------------
@bp.orchestration_trigger(context_name="context")
def orchestrator(context: df.DurableOrchestrationContext):
    """Main orchestrator for the data extraction pipeline"""

    # Get pipeline parameters
    params = context.get_input()
    manufacturer = params.get("manufacturer", "recom")
    product_types = params.get(
        "product_types", ["dc-dc-converters", "ac-dc-power-supplies"]
    )

    results = {}

    # Execute each step in sequence
    for product_type in product_types:
        # Step 1: Scrape series
        series_result = yield context.call_activity(
            "scrape_series",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_series"] = series_result

        # Step 2: Scrape products
        products_result = yield context.call_activity(
            "scrape_products",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_products"] = products_result

        # Step 3: Download PDFs
        pdf_result = yield context.call_activity(
            "download_pdfs",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_pdfs"] = pdf_result

        # Step 4: Extract data from PDFs
        extract_result = yield context.call_activity(
            "extract_pdf_data",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_extracted"] = extract_result

        # Step 5: Structure data
        structure_result = yield context.call_activity(
            "extract_structured_data",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_structured"] = structure_result

        # Step 6: Validate data
        validate_result = yield context.call_activity(
            "validate_data",
            {"manufacturer": manufacturer, "product_type": product_type},
        )
        results[f"{product_type}_validated"] = validate_result

    return results