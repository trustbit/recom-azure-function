import logging
import io
import json
import base64
import azure.functions as func
import pandas as pd
import requests
import tempfile

from shared.environment import AzureEnvironment

def extract_text_from_pdf(env, pdf_hash):
    """Extract text from PDF using Azure Document Intelligence (Form Recognizer)"""
    # Get PDF content from storage
    pdf_content = None
    with env.storage.read_cas(pdf_hash) as f:
        pdf_content = f.read()
    
    if not pdf_content:
        return None
    
    # Encode PDF for API call
    base64_encoded_pdf = base64.b64encode(pdf_content).decode()
    
    # Get Azure Document Intelligence API key and endpoint from environment
    endpoint = env.get_config("DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = env.get_config("DOCUMENT_INTELLIGENCE_KEY")
    
    # Prepare request
    url = f"{endpoint}/documentintelligence/documentModels/prebuilt-layout:analyze?api-version=2023-07-31"
    headers = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": key
    }
    body = {
        "base64Source": base64_encoded_pdf
    }
    
    # Call Azure Document Intelligence API
    response = requests.post(url, headers=headers, json=body)
    
    if response.status_code == 202:  # Accepted - async operation
        operation_location = response.headers["Operation-Location"]
        
        # Poll for results
        result_headers = {"Ocp-Apim-Subscription-Key": key}
        status = "running"
        max_retries = 50
        retries = 0
        
        while status == "running" and retries < max_retries:
            result_response = requests.get(operation_location, headers=result_headers)
            result = result_response.json()
            status = result.get("status", "")
            
            if status == "succeeded":
                # Extract and combine text from all pages
                pages = result.get("analyzeResult", {}).get("pages", [])
                text_content = ""
                
                for page in pages:
                    page_content = ""
                    lines = page.get("lines", [])
                    
                    for line in lines:
                        page_content += line.get("content", "") + "\n"
                    
                    text_content += page_content + "\n\n"
                
                return text_content
            
            retries += 1
            import time
            time.sleep(1)  # Wait before polling again
    
    logging.error(f"Failed to extract text from PDF: {response.text}")
    return None

def extract_pdfs_for_manufacturer(env, manufacturer, product_type):
    """Extract text from PDFs for a specific manufacturer and product type"""
    prev_step_name = f"{manufacturer}3_download_pdf"
    current_step_name = f"{manufacturer}4_extract_pdf_data"
    file_name = f"{product_type}.csv"
    
    # Check if previous step data exists
    if not env.storage.mutable_data_exists(prev_step_name, file_name):
        logging.warning(f"No data found for {manufacturer} {product_type}")
        return {
            "success": False,
            "error": "Previous step data not found",
            "count": 0
        }
    
    # Load PDF hash index if exists
    index_file = "latex_hash_index.json"
    hash_index = {}
    if env.storage.mutable_data_exists(current_step_name, index_file):
        hash_index = env.storage.load_json(current_step_name, index_file)
    
    # Load product data
    df = env.storage.load_df(prev_step_name, file_name)
    df["latex_hash"] = None
    
    # Process each product
    extract_count = 0
    for i, (idx, row) in enumerate(df.iterrows()):
        try:
            pdf_hash = row.get("pdf_hash")
            
            # Skip if PDF hash is missing
            if pd.isna(pdf_hash) or not pdf_hash:
                continue
            
            # Check if we already have extracted this PDF
            if pdf_hash in hash_index:
                df.at[idx, "latex_hash"] = hash_index[pdf_hash]
                continue
            
            # Extract text from PDF
            logging.info(f"Processing PDF file: {pdf_hash}")
            text_content = extract_text_from_pdf(env, pdf_hash)
            
            if text_content:
                # Save extracted text to storage
                byte_stream = io.BytesIO(text_content.encode('utf-8'))
                latex_hash = env.storage.save_cas(byte_stream)
                
                # Update dataframe and hash index
                df.at[idx, "latex_hash"] = latex_hash
                hash_index[pdf_hash] = latex_hash
                extract_count += 1
            
            # Save hash index and dataframe periodically
            if i % 10 == 0:
                env.storage.save_json(current_step_name, index_file, hash_index)
                env.storage.save_df(current_step_name, file_name, df)
                logging.info(f"Processed {i}/{len(df)} items for {manufacturer} {product_type}")
                
        except Exception as e:
            logging.error(f"Error processing item {i}: {str(e)}")
    
    # Save final results
    env.storage.save_json(current_step_name, index_file, hash_index)
    env.storage.save_df(current_step_name, file_name, df)
    
    return {
        "success": True,
        "count": extract_count,
        "total": len(df),
        "step_name": current_step_name,
        "file_name": file_name
    }

def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info('Extract PDF data function processing a request.')
    
    try:
        # Get parameters from request
        manufacturer = params.get('manufacturer', 'recom')
        product_type = params.get('product_type', 'dc-dc-converters')
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Extract PDF data
        result = extract_pdfs_for_manufacturer(env, manufacturer, product_type)
        
        return {
            "success": result["success"],
            "manufacturer": manufacturer,
            "product_type": product_type,
            "count": result.get("count", 0),
            "total": result.get("total", 0),
            "step_name": result.get("step_name", ""),
            "file_name": result.get("file_name", ""),
            "error": result.get("error", "")
        }
    except Exception as e:
        logging.error(f"Error in extract_pdf_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get('manufacturer', 'recom'),
            "product_type": params.get('product_type', 'dc-dc-converters')
        }