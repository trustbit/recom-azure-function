import logging
import json
import pandas as pd
import azure.functions as func
from playwright.async_api import async_playwright
import asyncio
from bs4 import BeautifulSoup

from shared.environment import AzureEnvironment

# URL templates for different manufacturers
MANUFACTURER_URLS = {
    "recom": {
        "dc-dc-converters": "https://recom-power.com/en/products/dc-dc-converters/rec-c-dc-dc-converters.html",
        "ac-dc-power-supplies": "https://recom-power.com/en/products/ac-dc-power-supplies/rec-c-ac-dc-power-supplies.html",
    },
    "traco": {
        "dc-dc-converters": "https://www.tracopower.com/product-finder/dc-dc",
        "ac-dc-power-supplies": "https://www.tracopower.com/product-finder/ac-dc",
    },
    "xppower": {
        "dc-dc-converters": "https://www.xppower.com/products/dc-dc-converters",
        "ac-dc-power-supplies": "https://www.xppower.com/products/ac-dc-power-supplies",
    }
}

async def parse_recom_series(page):
    """Parse RECOM power series from webpage"""
    # Wait for series items to load
    await page.wait_for_selector('tr[itemscope][itemtype="https://schema.org/ListItem"]', timeout=10000)
    
    # Get all series rows
    rows = await page.query_selector_all('tr[itemscope][itemtype="https://schema.org/ListItem"]')
    series_list = []
    
    for row in rows:
        try:
            # Extract data from each row
            image_url = await (await row.query_selector('.productSeriesImage img')).get_attribute('src')
            product_link = await (await row.query_selector('td:nth-child(3) a')).get_attribute('href')
            product_name = await (await row.query_selector('td:nth-child(3) a span')).inner_text()
            power = await (await row.query_selector('td:nth-child(4)')).inner_text()
            vin = await (await row.query_selector('td:nth-child(5)')).inner_text()
            main_vout = await (await row.query_selector('td:nth-child(6)')).inner_text()
            mounting_type = await (await row.query_selector('td:nth-child(7)')).inner_text()
            package_style = await (await row.query_selector('td:nth-child(8)')).inner_text()
            datasheet_link = await (await row.query_selector('td.additional-column a.btn')).get_attribute('href')
            
            series_list.append({
                'image_url': image_url,
                'product_link': product_link,
                'product_name': product_name,
                'power': power,
                'vin': vin,
                'main_vout': main_vout,
                'mounting_type': mounting_type,
                'package_style': package_style,
                'datasheet_link': datasheet_link
            })
        except Exception as e:
            logging.warning(f"Error processing a row: {str(e)}")
            continue
            
    return series_list

async def parse_traco_series(page):
    """Parse Traco Power series from webpage"""
    html_content = await page.content()
    soup = BeautifulSoup(html_content, 'html.parser')
    
    articles = soup.select('div.series-container article.series')
    series_list = []
    
    for article in articles:
        data = {}
        
        # Product Series
        product_series = article.select_one('.col-1 .product-title a')
        if product_series:
            data['product_series'] = product_series.text
            data['series_url'] = 'https://www.tracopower.com' + product_series['href'] if '/series/' in product_series['href'] else None
        
        # Power
        power = article.select_one('.col-2 .field--name-field-power') or article.select_one('.col-2 .field--name-field-power-superseries')
        data['series_power_variants'] = power.text if power else None
        
        # Input/Output voltages and other info
        input_voltage = article.select_one('.col-3 .field--name-field-input-voltage')
        data['series_input_voltage_variants'] = input_voltage.text if input_voltage else None
        
        output_voltage = article.select_one('.col-4 .field--name-field-output-voltage')
        data['series_output_voltage_variants'] = output_voltage.text if output_voltage else None
        
        short_description = article.select_one('.col-5 .field--name-field-short-description')
        data['series_short_description'] = short_description.text.strip() if short_description else None
        
        datasheet = article.select_one('.col-6 .field--name-field-datasheets a')
        data['datasheet_link'] = 'https://www.tracopower.com' + datasheet['href'] if datasheet else None
        
        # Images
        dimensions_img = article.select_one('.image-dimensions img')
        data['dimensions_image'] = 'https://www.tracopower.com' + dimensions_img['src'] if dimensions_img else None
        
        pinout_img = article.select_one('.image-pinout img')
        data['pinout_image'] = 'https://www.tracopower.com' + pinout_img['src'] if pinout_img else None
        
        series_list.append(data)
        
    return series_list

async def parse_xppower_series(page):
    """Parse XP Power series from webpage"""
    # Wait for products table
    await page.wait_for_selector('#products-table__table tbody tr', timeout=10000)
    
    # Get all series rows
    rows = await page.query_selector_all('#products-table__table tbody tr')
    series_list = []
    
    for row in rows:
        try:
            html = await row.inner_html()
            soup = BeautifulSoup(html, 'html.parser')
            
            img_elem = soup.find('img')
            name_elem = soup.find_all('a')[1] if len(soup.find_all('a')) > 1 else None
            power_elem = soup.find_all('td')[2] if len(soup.find_all('td')) > 2 else None
            phase_elem = soup.find_all('td')[3] if len(soup.find_all('td')) > 3 else None
            voltage_elem = soup.find_all('td')[4] if len(soup.find_all('td')) > 4 else None
            current_elem = soup.find_all('td')[5] if len(soup.find_all('td')) > 5 else None
            datasheet_elem = soup.find(lambda tag: tag.name == 'a' and 'Datasheet' in tag.text)
            
            series_list.append({
                "image": img_elem['src'] if img_elem and img_elem.has_attr('src') else "",
                "productCode": name_elem.get_text(strip=True) if name_elem else "",
                "url": name_elem['href'] if name_elem and name_elem.has_attr('href') else "",
                "power": power_elem.get_text(strip=True) if power_elem else "",
                "phase": phase_elem.get_text(strip=True) if phase_elem else "",
                "voltage": voltage_elem.get_text(strip=True) if voltage_elem else "",
                "current": current_elem.get_text(strip=True) if current_elem else "",
                "datasheet": datasheet_elem['href'] if datasheet_elem and datasheet_elem.has_attr('href') else "",
            })
        except Exception as e:
            logging.warning(f"Error processing row: {str(e)}")
            continue
    
    return series_list

async def scrape_series(manufacturer, product_type):
    """Scrape product series data from manufacturer website"""
    url = MANUFACTURER_URLS.get(manufacturer, {}).get(product_type)
    if not url:
        raise ValueError(f"Unsupported manufacturer/product type: {manufacturer}/{product_type}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to the URL
        await page.goto(url, wait_until="networkidle")
        
        series_data = []
        
        # Extract series data using appropriate parser for the manufacturer
        if manufacturer == "recom":
            series_data = await parse_recom_series(page)
        elif manufacturer == "traco":
            series_data = await parse_traco_series(page)
        elif manufacturer == "xppower":
            series_data = await parse_xppower_series(page)
        
        await browser.close()
        return series_data

def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info('Scrape series function processing a request.')
    
    try:
        # Get parameters from request
        manufacturer = params.get('manufacturer', 'recom')
        product_type = params.get('product_type', 'dc-dc-converters')
        
        # Initialize environment
        env = AzureEnvironment()
        
        # Run scraper
        series_data = asyncio.run(scrape_series(manufacturer, product_type))
        
        # Convert to DataFrame
        df = pd.DataFrame(series_data)
        
        # Save to storage
        step_name = f"{manufacturer}1_scrape_series"
        file_name = f"{product_type}.csv"
        env.storage.save_df(step_name, file_name, df)
        
        return {
            "success": True,
            "manufacturer": manufacturer,
            "product_type": product_type,
            "count": len(series_data),
            "step_name": step_name,
            "file_name": file_name
        }
    except Exception as e:
        logging.error(f"Error in scrape_series: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get('manufacturer', 'recom'),
            "product_type": params.get('product_type', 'dc-dc-converters')
        }