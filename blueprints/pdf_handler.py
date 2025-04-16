import logging
import io
import azure.functions as func
import pandas as pd
import requests
import PyPDF2
import fitz  # PyMuPDF
from shared.environment import AzureEnvironment

# Create blueprint instance
bp = func.Blueprint()


# --------------- Download PDFs Activity Function ---------------
@bp.activity_trigger(input_name="input")
def download_pdfs(input: dict) -> dict:
    """Activity function to download PDF files"""
    logging.info(
        f"Downloading PDFs for {input['manufacturer']} {input['product_type']}"
    )

    try:
        # Get parameters
        manufacturer = input.get("manufacturer", "recom")
        product_type = input.get("product_type", "dc-dc-converters")

        # Initialize environment
        env = AzureEnvironment()

        # Load products data with datasheet links
        step_name = f"{manufacturer}2_scrape_products"
        file_name = f"{product_type}.csv"
        products_df = env.storage.load_df(step_name, file_name)

        # Filter out rows without datasheet links
        products_df = products_df[
            products_df["datasheet_link"].notna()
            & (products_df["datasheet_link"] != "")
        ]

        # Download each PDF
        total_pdfs = len(products_df)
        downloaded = 0
        failures = 0

        for index, product in products_df.iterrows():
            try:
                # Get datasheet link
                datasheet_link = product["datasheet_link"]
                if not datasheet_link:
                    continue

                # Generate filename
                product_code = (
                    product["product_code"].replace("/", "_").replace("\\", "_")
                )
                pdf_filename = f"{product_code}.pdf"

                # Download PDF
                response = requests.get(datasheet_link, timeout=30)
                if response.status_code == 200:
                    # Save PDF to blob storage
                    container_name = f"{manufacturer}3_download_pdfs"
                    env.storage.save_blob(
                        container_name,
                        f"{product_type}/{pdf_filename}",
                        response.content,
                    )
                    downloaded += 1
                else:
                    logging.warning(
                        f"Failed to download PDF for {product_code}: HTTP {response.status_code}"
                    )
                    failures += 1
            except Exception as e:
                logging.warning(
                    f"Error downloading PDF for {product.get('product_code', 'unknown')}: {str(e)}"
                )
                failures += 1

        return {
            "success": True,
            "manufacturer": manufacturer,
            "product_type": product_type,
            "total_pdfs": total_pdfs,
            "downloaded": downloaded,
            "failures": failures,
        }
    except Exception as e:
        logging.error(f"Error in download_pdfs: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": input.get("manufacturer", "recom"),
            "product_type": input.get("product_type", "dc-dc-converters"),
        }


# --------------- Extract PDF Data Activity Function ---------------
@bp.activity_trigger(input_name="input")
def extract_pdf_data(input: dict) -> dict:
    """Activity function to extract data from PDF files"""
    logging.info(
        f"Extracting PDF data for {input['manufacturer']} {input['product_type']}"
    )

    try:
        # Get parameters
        manufacturer = input.get("manufacturer", "recom")
        product_type = input.get("product_type", "dc-dc-converters")

        # Initialize environment
        env = AzureEnvironment()

        # Get list of PDFs from the container
        container_name = f"{manufacturer}3_download_pdfs"
        pdf_files = env.storage.list_blobs(container_name, f"{product_type}/")

        extracted_data = []
        for pdf_blob in pdf_files:
            try:
                # Get filename from blob path
                filename = pdf_blob.name.split("/")[-1]
                product_code = filename.replace(".pdf", "").replace("_", "/")

                # Download PDF content
                pdf_content = env.storage.get_blob(container_name, pdf_blob.name)

                # Extract text from PDF
                pdf_bytes = io.BytesIO(pdf_content)
                text = ""

                # Try using PyPDF2 first
                try:
                    reader = PyPDF2.PdfReader(pdf_bytes)
                    for page_num in range(len(reader.pages)):
                        text += reader.pages[page_num].extract_text() + "\n"
                except Exception as e:
                    # If PyPDF2 fails, try PyMuPDF (fitz)
                    pdf_bytes.seek(0)
                    try:
                        doc = fitz.open(stream=pdf_bytes.read(), filetype="pdf")
                        for page_num in range(doc.page_count):
                            text += doc.load_page(page_num).get_text() + "\n"
                        doc.close()
                    except Exception as e2:
                        logging.warning(
                            f"Both PDF extraction methods failed for {filename}: {str(e2)}"
                        )
                        continue

                # Save extracted text
                extracted_data.append(
                    {
                        "product_code": product_code,
                        "filename": filename,
                        "extracted_text": text,
                    }
                )

            except Exception as e:
                logging.warning(f"Error extracting text from {pdf_blob.name}: {str(e)}")
                continue

        # Convert to DataFrame and save
        if extracted_data:
            df = pd.DataFrame(extracted_data)

            step_name = f"{manufacturer}4_extract_pdf_data"
            file_name = f"{product_type}.csv"
            env.storage.save_df(step_name, file_name, df)

            return {
                "success": True,
                "manufacturer": manufacturer,
                "product_type": product_type,
                "processed_pdfs": len(extracted_data),
                "step_name": step_name,
                "file_name": file_name,
            }
        else:
            return {
                "success": False,
                "error": "No data extracted from PDFs",
                "manufacturer": manufacturer,
                "product_type": product_type,
            }
    except Exception as e:
        logging.error(f"Error in extract_pdf_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": input.get("manufacturer", "recom"),
            "product_type": input.get("product_type", "dc-dc-converters"),
        }
