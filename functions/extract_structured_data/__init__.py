import logging
import json
import base64
import pandas as pd
import hashlib
from openai import OpenAI
import azure.functions as func

from shared.environment import AzureEnvironment
from shared.model import PowerConverterList, PowerConverterModel


def group_product_fields(records):
    """Group common fields to reduce context size"""
    common_fields = {}

    for k in list(records[0].keys()):
        if all([r[k] == records[0][k] for r in records]):
            common_fields[k] = records[0][k]
            # erase from all records
            for r in records:
                del r[k]

    ingest_data = {"common_series_info": common_fields, "products": records}

    return ingest_data


def read_pdf_as_base64(env, hash_str):
    """Read PDF content as base64 string"""
    with env.storage.read_cas(hash_str) as file:
        return base64.standard_b64encode(file.read()).decode("utf-8")


def extract_document_content(env, pdf_content, json_data, count):
    """
    Extract structured data from PDF content using OpenAI
    """
    # Initialize OpenAI client from environment
    api_key = env.get_config("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)

    system_prompt = (
        "You are an assistant specialized in extracting technical specifications from datasheets of AC/DC and DC/DC converters. "
        "You will be prompted with the contents of a PDF datasheet. Your task is to extract key performance metrics, "
        "technical parameters, protection features, and other relevant specifications from the datasheet. "
        "For each extracted metric, also provide the unit of measurement (e.g., 'V' for voltage, 'A' for current) "
        "and, where applicable, the condition under which the metric was measured (e.g., nominal voltage, full load, etc.). "
        "If the value is given in a scaled form (e.g., '3.3kV', '5mA'), extract the full value in standard units "
        "(e.g., '3300V', '0.005A'). Additionally, identify time-dependent parameters like recovery time, voltage dips, "
        "or isolation time and provide the time in milliseconds, seconds, etc. as appropriate. "
        "Extract as many details as possible, including protection types (e.g., OCP, OVP), input/output voltage ranges, "
        "load regulation, and operational temperature ranges."
        "\n\n"
        "Your goal is to capture all relevant specifications and operational details from the datasheet "
        f"in a clear and structured format for {count} following products: \n\n```json\n{json_data}\n```"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": pdf_content},
    ]

    # Request completion from OpenAI
    response = client.chat.completions.create(
        model="gpt-4o-2024-08-06",
        messages=messages,
        response_format={"type": "json_object"},  # Request JSON response
    )

    # Parse response into PowerConverterList model
    result_json = response.choices[0].message.content
    try:
        parsed_response = PowerConverterList.model_validate_json(result_json)
        return parsed_response
    except Exception as e:
        logging.error(f"Error parsing OpenAI response: {str(e)}")
        logging.error(f"Raw response: {result_json[:500]}...")  # Log first 500 chars
        raise


def extract_with_caching(env, step, pdf_content, json_data, count):
    """Extract structured data with caching to minimize API calls"""
    # Compute hash for caching
    messages_json = json.dumps(
        [
            {"content": pdf_content[:1000]},  # Use first 1000 chars as approximation
            {"content": json_data},
        ]
    )
    cache_key = hashlib.sha1(messages_json.encode()).hexdigest()
    cache_name = f"cache_{cache_key}.json"

    # Check if cached response exists
    if env.storage.mutable_data_exists(step, cache_name):
        logging.info(f"Using cached extraction for {cache_key}")
        cached_json = env.storage.load_mutable_text(step, cache_name)
        return PowerConverterList.model_validate_json(cached_json)

    # Perform extraction
    logging.info(f"Performing new extraction for {count} products")
    response = extract_document_content(env, pdf_content, json_data, count)
    result_json = response.choices[0].message.content

    # Cache result
    env.storage.write_mutable_data(step, cache_name, result_json)

    return PowerConverterList.model_validate_json(result_json)


def extract_structured_data_for_manufacturer(env, manufacturer, product_type):
    """Extract structured data for a specific manufacturer and product type"""
    prev_step_name = f"{manufacturer}4_extract_pdf_data"
    current_step_name = f"{manufacturer}5_structured_product_data"
    schema_version = "v11"  # Use the latest schema version

    version_step_name = f"{prev_step_name}/ver_2"  # Use the appropriate schema version
    current_full_step = f"{current_step_name}/schema_{schema_version}"

    # Check if previous step data exists
    file_name = f"{product_type}.csv"
    if not env.storage.mutable_data_exists(version_step_name, file_name):
        logging.warning(f"No data found for {manufacturer} {product_type}")
        return {"success": False, "error": "Previous step data not found", "count": 0}

    # Load product data
    df = env.storage.load_df(version_step_name, file_name)

    # Add converter type based on product type
    if "type" not in df.columns:
        if product_type == "dc-dc-converters":
            df["type"] = "DCDC"
        elif product_type == "ac-dc-power-supplies":
            df["type"] = "ACDC"

    # Deduplicate by part_number to avoid redundant processing
    df = df.drop_duplicates(subset=["part_number"])

    # Save index for future reference
    env.storage.save_df(current_full_step, f"index.csv", df)

    # Group by PDF hash (underlying PDF file)
    pdf_groups = df.groupby("pdf_hash")

    extract_count = 0
    for pdf_hash, group in pdf_groups:
        try:
            # Skip if PDF hash is missing
            if pd.isna(pdf_hash) or not pdf_hash:
                continue

            # Set up file names
            struct_file_name = f"{pdf_hash}_pdf.json"
            group_file_name = f"{pdf_hash}_html.json"

            # Skip if already processed
            if env.storage.mutable_data_exists(current_full_step, struct_file_name):
                logging.info(f"Skipping {struct_file_name}, already processed")
                continue

            # Get number of products in this group
            group_count = len(group)
            logging.info(f"Processing group with {group_count} items")

            # Get latex content
            latex_hash = group["latex_hash"].iloc[0]
            with env.storage.read_cas(latex_hash) as f:
                latex_content = f.read().decode("utf-8")

            # Prepare subset of data for context
            subset_df = group[
                [
                    "Mounting Type",
                    "Certifications",
                    "Isolation (kV)",
                    "type",
                    "series",
                    "part_number",
                    "power",
                    "isolation",
                    "vin",
                    "main_vout",
                    "package_style",
                ]
            ].copy()

            # Convert to JSON with grouped common fields
            records = subset_df.to_dict(orient="records")
            ingest_data = group_product_fields(records)
            json_data = json.dumps(ingest_data, ensure_ascii=False, indent=2)

            # Extract structured data
            structured_data = extract_with_caching(
                env, current_full_step, latex_content, json_data, group_count
            )

            # Save structured data
            env.storage.write_mutable_data(
                current_full_step, struct_file_name, structured_data.model_dump_json()
            )

            # Save group data for reference
            env.storage.write_mutable_data(
                current_full_step, group_file_name, group.to_json(orient="records")
            )

            extract_count += 1
            logging.info(f"Extracted {len(structured_data.power_converters)} items")

        except Exception as e:
            logging.error(f"Error processing PDF hash {pdf_hash}: {str(e)}")

    return {
        "success": True,
        "count": extract_count,
        "total": len(pdf_groups),
        "step_name": current_full_step,
        "schema_version": schema_version,
    }


def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info("Extract structured data function processing a request.")

    try:
        # Get parameters from request
        manufacturer = params.get("manufacturer", "recom")
        product_type = params.get("product_type", "dc-dc-converters")

        # Initialize environment
        env = AzureEnvironment()

        # Extract structured data
        result = extract_structured_data_for_manufacturer(
            env, manufacturer, product_type
        )

        return {
            "success": result["success"],
            "manufacturer": manufacturer,
            "product_type": product_type,
            "count": result.get("count", 0),
            "total": result.get("total", 0),
            "step_name": result.get("step_name", ""),
            "schema_version": result.get("schema_version", ""),
            "error": result.get("error", ""),
        }
    except Exception as e:
        logging.error(f"Error in extract_structured_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get("manufacturer", "recom"),
            "product_type": params.get("product_type", "dc-dc-converters"),
        }
