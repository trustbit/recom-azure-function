import logging
import asyncio
import io
import aiohttp
import pandas as pd
import azure.functions as func

from shared.environment import AzureEnvironment

async def download_file_to_bytesio(url):
    """Download file from URL to BytesIO object"""
    if not url:
        return None
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    file_content = io.BytesIO(content)
                    file_content.seek(0)
                    return file_content
                else:
                    logging.warning(f"Failed to download {url}, status: {response.status}")
                    return None
    except Exception as e:
        logging.error(f"Error downloading file from {url}: {str(e)}")
        return None

async def download_pdfs_for_manufacturer(env, manufacturer, product_type):
    """Download PDFs for a specific manufacturer and product type"""
    prev_step_name = f"{manufacturer}2_scrape_products"
    current_step_name = f"{manufacturer}3_download_pdf"
    file_name = f"{product_type}.csv"
    
    # Check if previous step data exists
    if not env.storage.mutable_data_exists(prev_step_name, file_name):
        logging.warning(f"No data found for {manufacturer} {product_type}")
        return {
            "success": False,
            "error": "Previous step data not found",
            "count": 0
        }
    
    # Check if current step data exists and load PDF hashes
    pdf_urls_hash = {}
    image_urls_hash = {}
    if env.storage.mutable_data_exists(current_step_name, file_name):
        df = env.storage.load_df(current_step_name, file_name)
        for _, row in df.iterrows():
            # Get datasheet link column - different names by manufacturer
            if manufacturer == "recom":
                datasheet_url = row.get('datasheet_link', None)
                image_url = row.get('image_url', None)
            elif manufacturer == "traco":
                datasheet_url = row.get('datasheet_link', None)
                image_url = None
            elif manufacturer == "xppower":
                datasheet_url = row.get('datasheet', None)
                image_url = row.get('image', None)
            
            pdf_hash = row.get('pdf_hash', None)
            image_hash = row.get('image_hash', None)
            
            if datasheet_url and pdf_hash:
                pdf_urls_hash[datasheet_url] = pdf_hash
            if image_url and image_hash:
                image_urls_hash[image_url] = image_hash
    
    # Load product data
    df = env.storage.load_df(prev_step_name, file_name)
    pdf_hash_column = [None] * len(df)
    image_hash_column = [None] * len(df)
    
    # Process each product
    download_count = 0
    for i, (idx, row) in enumerate(df.iterrows()):
        try:
            # Get datasheet link and image URL based on manufacturer
            if manufacturer == "recom":
                datasheet_url = row.get('datasheet_link', None)
                image_url = row.get('image_url', None)
            elif manufacturer == "traco":
                datasheet_url = row.get('datasheet_link', None)
                if datasheet_url and not datasheet_url.startswith(('http://', 'https://')):
                    datasheet_url = f"https://www.tracopower.com{datasheet_url}"
                image_url = None
            elif manufacturer == "xppower":
                datasheet_url = row.get('datasheet', None)
                image_url = row.get('image', None)
            
            # Download PDF if not already cached
            pdf_hash = pdf_urls_hash.get(datasheet_url, None)
            if not pdf_hash and datasheet_url:
                data = await download_file_to_bytesio(datasheet_url)
                if data:
                    pdf_hash = env.storage.save_cas(data)
                    pdf_urls_hash[datasheet_url] = pdf_hash
                    download_count += 1
            pdf_hash_column[idx] = pdf_hash
            
            # Download image if not already cached
            image_hash = image_urls_hash.get(image_url, None)
            if not image_hash and image_url:
                data = await download_file_to_bytesio(image_url)
                if data:
                    image_hash = env.storage.save_cas(data)
                    image_urls_hash[image_url] = image_hash
            image_hash_column[idx] = image_hash
            
            # Log progress periodically
            if i % 10 == 0:
                logging.info(f"Processed {i}/{len(df)} items for {manufacturer} {product_type}")
        except Exception as e:
            logging.error(f"Error processing item {i}: {str(e)}")
    
    # Update dataframe with hash values
    df["pdf_hash"] = pdf_hash_column
    df["image_hash"] = image_hash_column
    
    # Save updated dataframe
    env.storage.save_df(current_step_name, file_name, df)
    
    return {
        "success": True,
        "count": download_count,
        "total": len(df),
        "step_name": current_step_name,
        "file_name": file_name
    }

def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info('Download PDFs function processing a request.')
    
    try:
        # Get parameters from request
        manufacturer = params.get('manufacturer', 'recom')
        product_type = params.get('product_type', 'dc-dc-converters')
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Download PDFs
        result = asyncio.run(download_pdfs_for_manufacturer(env, manufacturer, product_type))
        
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
        logging.error(f"Error in download_pdfs: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get('manufacturer', 'recom'),
            "product_type": params.get('product_type', 'dc-dc-converters')
        }