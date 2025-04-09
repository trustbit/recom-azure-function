import logging
import re
import azure.functions as func
import pandas as pd
from shared.environment import AzureEnvironment
from shared.model import Product, Series, PowerConverter

# Create blueprint instance
bp = func.Blueprint()

# --------------- Extract Structured Data Activity Function ---------------
@bp.activity_trigger(input_name="input")
def extract_structured_data(input: dict) -> dict:
    """Activity function to extract structured data from PDF text"""
    logging.info(f"Extracting structured data for {input['manufacturer']} {input['product_type']}")
    
    try:
        # Get parameters
        manufacturer = input.get("manufacturer", "recom")
        product_type = input.get("product_type", "dc-dc-converters")
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Load extracted PDF data
        step_name = f"{manufacturer}4_extract_pdf_data"
        file_name = f"{product_type}.csv"
        pdf_data_df = env.storage.load_df(step_name, file_name)
        
        # Load product data for reference
        product_step_name = f"{manufacturer}2_scrape_products"
        product_file_name = f"{product_type}.csv"
        products_df = env.storage.load_df(product_step_name, product_file_name)
        
        structured_data = []
        
        for _, row in pdf_data_df.iterrows():
            try:
                product_code = row["product_code"]
                text = row["extracted_text"]
                
                # Find matching product in products_df
                product_info = products_df[products_df["product_code"] == product_code]
                
                # Extract structured data based on the manufacturer and product type
                if manufacturer == "recom":
                    data = extract_recom_structured_data(text, product_info)
                elif manufacturer == "traco":
                    data = extract_traco_structured_data(text, product_info)
                elif manufacturer == "xppower":
                    data = extract_xppower_structured_data(text, product_info)
                else:
                    data = {}
                
                # Add product code
                data["product_code"] = product_code
                
                structured_data.append(data)
                
            except Exception as e:
                logging.warning(f"Error processing structured data for {row.get('product_code', 'unknown')}: {str(e)}")
                continue
        
        # Convert to DataFrame and save
        if structured_data:
            df = pd.DataFrame(structured_data)
            
            step_name = f"{manufacturer}5_extract_structured_data"
            file_name = f"{product_type}.csv"
            env.storage.save_df(step_name, file_name, df)
            
            return {
                "success": True,
                "manufacturer": manufacturer,
                "product_type": product_type,
                "processed_items": len(structured_data),
                "step_name": step_name,
                "file_name": file_name,
            }
        else:
            return {
                "success": False,
                "error": "No structured data extracted",
                "manufacturer": manufacturer,
                "product_type": product_type,
            }
    except Exception as e:
        logging.error(f"Error in extract_structured_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": input.get("manufacturer", "recom"),
            "product_type": input.get("product_type", "dc-dc-converters"),
        }

def extract_recom_structured_data(text, product_info):
    """Extract structured data from RECOM PDF text"""
    data = {}
    
    # Extract common specifications
    input_voltage_pattern = r"Input voltage range\s*[:]\s*([\d\.\-]+)\s*to\s*([\d\.]+)\s*V"
    output_voltage_pattern = r"Output voltage\s*[:]\s*([\d\.]+)\s*V"
    efficiency_pattern = r"Efficiency\s*[:]\s*([\d\.]+)\s*%"
    isolation_pattern = r"I/O isolation\s*[:]\s*([\d\.]+)\s*V"
    
    input_match = re.search(input_voltage_pattern, text)
    if input_match:
        data["input_voltage_min"] = input_match.group(1)
        data["input_voltage_max"] = input_match.group(2)
    
    output_match = re.search(output_voltage_pattern, text)
    if output_match:
        data["output_voltage"] = output_match.group(1)
    
    efficiency_match = re.search(efficiency_pattern, text)
    if efficiency_match:
        data["efficiency"] = efficiency_match.group(1)
    
    isolation_match = re.search(isolation_pattern, text)
    if isolation_match:
        data["isolation"] = isolation_match.group(1)
    
    # Add basic info from product_info if available
    if not product_info.empty:
        product = product_info.iloc[0]
        data["series_name"] = product.get("series_name", "")
        data["description"] = product.get("description", "")
        data["series_power"] = product.get("series_power", "")
        data["series_mounting_type"] = product.get("series_mounting_type", "")
        data["series_package_style"] = product.get("series_package_style", "")
    
    return data

def extract_traco_structured_data(text, product_info):
    """Extract structured data from Traco PDF text"""
    data = {}
    
    # Extract common specifications
    input_voltage_pattern = r"Input voltage range\s*[:]\s*([\d\.\-]+)\s*to\s*([\d\.]+)\s*V"
    output_voltage_pattern = r"Output voltage\s*[:]\s*([\d\.]+)\s*V"
    max_power_pattern = r"Maximum output power\s*[:]\s*([\d\.]+)\s*W"
    isolation_pattern = r"Isolation test voltage\s*[:]\s*([\d\.]+)\s*V"
    temperature_pattern = r"Operating temperature range\s*[:]\s*([\-\d\.]+)\s*to\s*([\d\.]+)\s*°C"
    
    input_match = re.search(input_voltage_pattern, text)
    if input_match:
        data["input_voltage_min"] = input_match.group(1)
        data["input_voltage_max"] = input_match.group(2)
    
    output_match = re.search(output_voltage_pattern, text)
    if output_match:
        data["output_voltage"] = output_match.group(1)
    
    power_match = re.search(max_power_pattern, text)
    if power_match:
        data["max_power"] = power_match.group(1)
    
    isolation_match = re.search(isolation_pattern, text)
    if isolation_match:
        data["isolation"] = isolation_match.group(1)
    
    temp_match = re.search(temperature_pattern, text)
    if temp_match:
        data["operating_temp_min"] = temp_match.group(1)
        data["operating_temp_max"] = temp_match.group(2)
    
    # Add basic info from product_info if available
    if not product_info.empty:
        product = product_info.iloc[0]
        data["series_name"] = product.get("series_name", "")
        data["efficiency"] = product.get("efficiency", "")
        data["power"] = product.get("power", "")
        data["series_description"] = product.get("series_description", "")
    
    return data

def extract_xppower_structured_data(text, product_info):
    """Extract structured data from XP Power PDF text"""
    data = {}
    
    # Extract common specifications
    input_voltage_pattern = r"Input Voltage Range\s*[:]\s*([\d\.\-]+)\s*to\s*([\d\.]+)\s*V"
    output_voltage_pattern = r"Output Voltage\s*[:]\s*([\d\.]+)\s*V"
    power_pattern = r"Output Power\s*[:]\s*([\d\.]+)\s*W"
    efficiency_pattern = r"Efficiency\s*[:]\s*([\d\.]+)\s*%"
    
    input_match = re.search(input_voltage_pattern, text)
    if input_match:
        data["input_voltage_min"] = input_match.group(1)
        data["input_voltage_max"] = input_match.group(2)
    
    output_match = re.search(output_voltage_pattern, text)
    if output_match:
        data["output_voltage"] = output_match.group(1)
    
    power_match = re.search(power_pattern, text)
    if power_match:
        data["output_power"] = power_match.group(1)
    
    efficiency_match = re.search(efficiency_pattern, text)
    if efficiency_match:
        data["efficiency"] = efficiency_match.group(1)
    
    # Add basic info from product_info if available
    if not product_info.empty:
        product = product_info.iloc[0]
        data["series_name"] = product.get("series_name", "")
        data["power"] = product.get("power", "")
        data["phase"] = product.get("phase", "")
        data["voltage"] = product.get("voltage", "")
        data["current"] = product.get("current", "")
    
    return data

# --------------- Validate Data Activity Function ---------------
@bp.activity_trigger(input_name="input")
def validate_data(input: dict) -> dict:
    """Activity function to validate the structured data"""
    logging.info(f"Validating data for {input['manufacturer']} {input['product_type']}")
    
    try:
        # Get parameters
        manufacturer = input.get("manufacturer", "recom")
        product_type = input.get("product_type", "dc-dc-converters")
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Load structured data
        step_name = f"{manufacturer}5_extract_structured_data"
        file_name = f"{product_type}.csv"
        data_df = env.storage.load_df(step_name, file_name)
        
        # Load product data for reference
        product_step_name = f"{manufacturer}2_scrape_products"
        product_file_name = f"{product_type}.csv"
        products_df = env.storage.load_df(product_step_name, product_file_name)
        
        # Create valid and invalid dataframes
        valid_data = []
        invalid_data = []
        
        for _, row in data_df.iterrows():
            # Validation rules
            is_valid = True
            validation_errors = []
            
            # Check if product code exists and matches product data
            product_code = row.get("product_code", "")
            if not product_code:
                is_valid = False
                validation_errors.append("Missing product code")
            else:
                # Find matching product in products_df
                matching_products = products_df[products_df["product_code"] == product_code]
                if matching_products.empty:
                    is_valid = False
                    validation_errors.append("Product code not found in scraped products")
            
            # Validate input voltage if available
            input_min = row.get("input_voltage_min")
            input_max = row.get("input_voltage_max")
            if pd.notna(input_min) and pd.notna(input_max):
                try:
                    min_val = float(input_min)
                    max_val = float(input_max)
                    if min_val > max_val:
                        is_valid = False
                        validation_errors.append("Input voltage min greater than max")
                except (ValueError, TypeError):
                    is_valid = False
                    validation_errors.append("Invalid input voltage values")
            
            # Validate output voltage if available
            output_voltage = row.get("output_voltage")
            if pd.notna(output_voltage):
                try:
                    float(output_voltage)
                except (ValueError, TypeError):
                    is_valid = False
                    validation_errors.append("Invalid output voltage value")
            
            # Add validation result
            row_dict = row.to_dict()
            row_dict["validation_errors"] = ", ".join(validation_errors) if validation_errors else ""
            
            if is_valid:
                valid_data.append(row_dict)
            else:
                invalid_data.append(row_dict)
        
        # Convert to DataFrames
        valid_df = pd.DataFrame(valid_data) if valid_data else pd.DataFrame()
        invalid_df = pd.DataFrame(invalid_data) if invalid_data else pd.DataFrame()
        
        # Save results
        valid_step_name = f"{manufacturer}6_validate_data"
        valid_file_name = f"{product_type}_valid.csv"
        invalid_file_name = f"{product_type}_invalid.csv"
        
        if not valid_df.empty:
            env.storage.save_df(valid_step_name, valid_file_name, valid_df)
        
        if not invalid_df.empty:
            env.storage.save_df(valid_step_name, invalid_file_name, invalid_df)
        
        # Create PowerConverter objects from valid data
        converters = []
        for _, row in valid_df.iterrows():
            try:
                # Create Series object
                series = Series(
                    name=row.get("series_name", ""),
                    power=row.get("series_power", ""),
                    description=row.get("series_description", "")
                )
                
                # Create Product object
                product = Product(
                    code=row.get("product_code", ""),
                    description=row.get("description", ""),
                    series=series
                )
                
                # Create PowerConverter object
                converter = PowerConverter(
                    product=product,
                    input_voltage_min=row.get("input_voltage_min"),
                    input_voltage_max=row.get("input_voltage_max"),
                    output_voltage=row.get("output_voltage"),
                    efficiency=row.get("efficiency"),
                    isolation=row.get("isolation"),
                    manufacturer=manufacturer
                )
                
                converters.append(converter)
            except Exception as e:
                logging.warning(f"Error creating PowerConverter for {row.get('product_code', 'unknown')}: {str(e)}")
                continue
        
        # Save PowerConverter objects to JSON
        json_step_name = f"{manufacturer}6_validate_data"
        json_file_name = f"{product_type}_converters.json"
        
        converter_dicts = [converter.to_dict() for converter in converters]
        env.storage.save_json(json_step_name, json_file_name, converter_dicts)
        
        return {
            "success": True,
            "manufacturer": manufacturer,
            "product_type": product_type,
            "total_items": len(data_df),
            "valid_items": len(valid_data),
            "invalid_items": len(invalid_data),
            "step_name": valid_step_name,
            "valid_file_name": valid_file_name,
            "invalid_file_name": invalid_file_name,
            "json_file_name": json_file_name,
        }
    except Exception as e:
        logging.error(f"Error in validate_data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": input.get("manufacturer", "recom"),
            "product_type": input.get("product_type", "dc-dc-converters"),
        }