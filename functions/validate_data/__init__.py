import logging
import json
import pandas as pd
import azure.functions as func

from shared.environment import AzureEnvironment
from shared.models import PowerConverterList

def validate_data_for_manufacturer(env, manufacturer, product_type):
    """Validate and merge extracted data for a specific manufacturer and product type"""
    prev_step_name = f"{manufacturer}5_structured_product_data"
    current_step_name = f"{manufacturer}9_validate_data"
    
    # Schema versions
    prev_schema_version = "v11"  # Use the latest schema version
    current_schema_version = "v8"  # Use the appropriate validation schema version
    
    # Full step paths
    prev_full_step = f"{prev_step_name}/schema_{prev_schema_version}"
    current_full_step = f"{current_step_name}/schema_{current_schema_version}"
    
    # Check if previous step data exists
    if not env.storage.mutable_data_exists(prev_full_step, "index.csv"):
        logging.warning(f"No index data found for {manufacturer} in {prev_full_step}")
        return {
            "success": False,
            "error": "Previous step data not found",
            "count": 0
        }
    
    # Load index data
    df = env.storage.load_df(prev_full_step, "index.csv")
    
    # Get unique PDF hashes
    pdf_hashes = df["pdf_hash"].unique()
    logging.info(f"Found {len(pdf_hashes)} unique PDF hashes in index")
    
    # List all files in the previous step
    all_files = env.storage.list_files(prev_full_step, "")
    logging.info(f"Found {len(all_files)} files in {prev_full_step}")
    
    # Process each PDF hash
    series = []
    total_records = 0
    
    for hash_str in pdf_hashes:
        if pd.isna(hash_str) or not hash_str:
            continue
            
        html_file = f"{hash_str}_html.json"
        pdf_file = f"{hash_str}_pdf.json"
        
        # Check if both files exist
        if html_file in all_files and pdf_file in all_files:
            logging.info(f"For hash {hash_str} both files are present")
        else:
            logging.warning(f"For hash {hash_str} files are missing: html={html_file in all_files}, pdf={pdf_file in all_files}")
            continue
        
        try:
            # Load PDF data (structured converter models)
            with env.storage.read_mutable_data(prev_full_step, pdf_file) as file:
                pdf_content = file.read().decode('utf-8')
                pdf_rows = PowerConverterList.model_validate_json(pdf_content)
            
            # Load HTML data (original product data)
            with env.storage.read_mutable_data(prev_full_step, html_file) as file:
                html_content = file.read().decode('utf-8')
                html_rows = json.loads(html_content)
            
            # Add to total record count
            total_records += len(pdf_rows.power_converters)
            
            # Create series entry
            serie = {
                "pdf": pdf_rows.model_dump(),
                "html": html_rows,
                "hash": hash_str
            }
            series.append(serie)
            
        except Exception as e:
            logging.error(f"Error processing hash {hash_str}: {str(e)}")
    
    # Save merged data
    merged_data = json.dumps(series, indent=2)
    env.storage.write_mutable_data(current_full_step, "merged.json", merged_data)
    
    logging.info(f"Validated {len(series)} groups with {total_records} products and saved to merged.json")
    
    return {
        "success": True,
        "count": len(series),
        "total_records": total_records,
        "step_name": current_full_step,
        "schema_version": current_schema_version
    }

def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info('Validate data function processing a request.')
    
    try:
        # Get parameters from request
        manufacturer = params.get('manufacturer', 'recom')
        product_type = params.get('product_type', 'dc-dc-converters')
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Validate data
        result = validate_data_for_manufacturer(env, manufacturer, product_type)
        
        return {
            "success": result["success"],
            "manufacturer": manufacturer,
            "product_type": product_type,
            "count": result.get("count", 0),
            "total_records": result.get("total_records", 0),
            "step_name": result.get("step_name", ""),
            "schema_version": result.get("schema_version", ""),
            "error": result.get("error", "")
        }
    except Exception as e:
        logging.error(f"Error in validate_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get('manufacturer', 'recom'),
            "product_type": params.get('product_type', 'dc-dc-converters')
        }