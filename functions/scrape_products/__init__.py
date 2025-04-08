import logging
import json
import pandas as pd
import azure.functions as func
from playwright.async_api import async_playwright
import asyncio
from bs4 import BeautifulSoup

from shared.environment import AzureEnvironment


async def scrape_recom_products(env, product_type):
    """Scrape RECOM products from series data"""
    # Load series data
    step_name = "recom1_scrape_series"
    file_name = f"{product_type}.csv"
    series_df = env.storage.load_df(step_name, file_name)

    all_products = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for _, series in series_df.iterrows():
            try:
                series_url = "https://recom-power.com/en/products" + series[
                    "product_link"
                ].strip(".")

                # Navigate to series page
                await page.goto(series_url, wait_until="networkidle")

                # Parse product table
                products = await parse_recom_product_table(page, series)
                all_products.extend(products)

                # Add delay between requests
                await asyncio.sleep(2)
            except Exception as e:
                logging.error(
                    f"Error scraping series {series.get('product_name', 'unknown')}: {str(e)}"
                )
                continue

        await browser.close()

    return all_products


async def parse_recom_product_table(page, series):
    """Parse RECOM product table from a series page"""
    products = []
    content = await page.content()
    soup = BeautifulSoup(content, "html.parser")

    # Get common specifications
    common_specs = soup.find("div", {"class": "product-features"})
    common_data = {}

    if common_specs:
        rows = common_specs.find_all("tr")
        # Process rows (skip header)
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) == 2:
                key = cells[0].text.strip()
                value = cells[1].text.strip()
                common_data[key] = value

    # Find product table
    table = soup.find("div", {"class": "product-container"})
    if not table:
        return []

    # Get column indices
    headers = table.find("tr", class_="headers")
    column_indices = {}
    for index, th in enumerate(headers.find_all("th")):
        span = th.find("span")
        if span and span.text.strip():
            column_name = span.text.strip().lower().replace(" ", "_")
            column_indices[column_name] = index

    # Extract products
    for row in table.find_all("tr")[1:]:
        try:
            cols = row.find_all("td")
            product = dict(common_data)  # Start with common data

            # Extract product info from columns
            if "part_number" in column_indices:
                product["part_number"] = (
                    cols[column_indices["part_number"]]
                    .find("span", {"class": "part-number"})
                    .text.strip()
                )
            if "power_(w)" in column_indices:
                product["power"] = cols[column_indices["power_(w)"]].text.strip()
            if "isolation" in column_indices:
                product["isolation"] = cols[column_indices["isolation"]].text.strip()
            if "vin_(v)" in column_indices:
                product["vin"] = cols[column_indices["vin_(v)"]].text.strip()
            if "main_vout_(v)" in column_indices:
                product["main_vout"] = cols[
                    column_indices["main_vout_(v)"]
                ].text.strip()
            if "iout_1_(ma)" in column_indices:
                product["iout1"] = cols[column_indices["iout_1_(ma)"]].text.strip()
            if "package_style" in column_indices:
                product["package_style"] = cols[
                    column_indices["package_style"]
                ].text.strip()

            # Find image URL
            image_div = None
            for col in cols:
                image_div = col.find("div", {"class": "image-download"})
                if image_div:
                    break
            product["image_url"] = (
                image_div.find("a", {"class": "rbutton"})["href"] if image_div else None
            )

            # Add series info
            product["datasheet_link"] = series["datasheet_link"]
            product["series"] = series["product_name"]

            products.append(product)
        except Exception as e:
            logging.warning(f"Error processing product row: {str(e)}")
            continue

    return products


async def scrape_traco_products(env, product_type):
    """Scrape Traco Power products from series data"""
    # Load series data
    step_name = "traco1_scrape_series"
    file_name = f"{product_type}.csv"
    series_df = env.storage.load_df(step_name, file_name)

    all_products = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for _, series in series_df.iterrows():
            try:
                if pd.isna(series["series_url"]):
                    continue

                # Navigate to series page
                await page.goto(series["series_url"], wait_until="networkidle")

                # Parse products
                html_content = await page.content()
                soup = BeautifulSoup(html_content, "html.parser")

                # Get series features
                features = soup.select_one(
                    "div.field--name-field-features div.field__items"
                )
                series_features = features.text.strip() if features else ""

                # Get product links
                product_links = soup.select(
                    "div.content article.model a.model-page-link"
                )

                for link in product_links:
                    try:
                        full_link = f"https://www.tracopower.com{link['href']}"

                        # Navigate to product page
                        await page.goto(full_link, wait_until="networkidle")
                        product_content = await page.content()
                        product_soup = BeautifulSoup(product_content, "html.parser")

                        # Extract product info
                        product = {}
                        product["product_url"] = full_link
                        product["series_url"] = series["series_url"]
                        product["features"] = series_features
                        product["product_series"] = series["product_series"]

                        # Basic info
                        part_number = product_soup.select_one(
                            "div.field--name-title h1"
                        )
                        product["part_number"] = (
                            part_number.text.strip() if part_number else ""
                        )

                        description = product_soup.select_one(
                            "div.field--name-field-description"
                        )
                        product["description"] = (
                            description.text.strip() if description else ""
                        )

                        # Datasheet link
                        datasheet_link = product_soup.find("a", string="Datasheet")
                        product["datasheet_link"] = (
                            datasheet_link["href"]
                            if datasheet_link
                            else series["datasheet_link"]
                        )

                        # Attributes
                        attribute_items = product_soup.select(
                            "div.model-attributes-container div.attribute-item"
                        )
                        for item in attribute_items:
                            label = item.select_one("div.attribute-label").text.strip()
                            value = item.select_one("div.attribute-value").text.strip()
                            product[label] = value

                        all_products.append(product)
                    except Exception as e:
                        logging.warning(
                            f"Error processing product {link.get('href', 'unknown')}: {str(e)}"
                        )
                        continue

                # Add delay between series
                await asyncio.sleep(2)
            except Exception as e:
                logging.error(
                    f"Error scraping series {series.get('product_series', 'unknown')}: {str(e)}"
                )
                continue

        await browser.close()

    return all_products


async def scrape_xppower_products(env, product_type):
    """Scrape XP Power products from series data"""
    # Load series data
    step_name = "xppower1_scrape_series"
    file_name = f"{product_type}.csv"
    series_df = env.storage.load_df(step_name, file_name)

    all_products = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for _, series in series_df.iterrows():
            try:
                series_url = series["url"]
                if not series_url:
                    continue

                # Navigate to series page
                await page.goto(series_url, wait_until="networkidle")

                # Wait for products table
                await page.wait_for_selector(
                    "table.min-w-full", state="visible", timeout=10000
                )

                # Get series description
                description = {}
                description_div = await page.query_selector(
                    "div.single-product .fadeup.fadeup-2"
                )
                if description_div:
                    html = await description_div.inner_html()
                    soup = BeautifulSoup(html, "html.parser")

                    title_elem = soup.select_one("h1.xp-markdown")
                    description["title"] = title_elem.text.strip() if title_elem else ""

                    subtitle_elem = soup.select_one("div.lg\\:text-lg p strong")
                    description["subtitle"] = (
                        subtitle_elem.text.strip() if subtitle_elem else ""
                    )

                    description_paragraphs = soup.select("div.space-y-7 p")
                    description["description"] = "\n\n".join(
                        [p.text.strip() for p in description_paragraphs]
                    )

                # Get features
                features = []
                features_header = await page.query_selector('h2:text("Features")')
                if features_header:
                    features_div = await features_header.evaluate(
                        'el => el.closest("div.fadeup-group")'
                    )
                    if features_div:
                        features_html = await page.evaluate(
                            "div => div.outerHTML", features_div
                        )
                        soup = BeautifulSoup(features_html, "html.parser")
                        feature_items = soup.select("div.fadeup ul li")
                        features = [
                            item.text.strip()
                            for item in feature_items
                            if item.text.strip()
                        ]

                # Get product rows
                table_html = await page.evaluate("""() => {
                    const table = document.querySelector('table.min-w-full');
                    return table ? table.outerHTML : '';
                }""")

                soup = BeautifulSoup(table_html, "html.parser")
                product_rows = soup.select("tbody tr")

                for row in product_rows:
                    try:
                        # Extract product data
                        td_elements = row.find_all("td")
                        if len(td_elements) < 6:
                            continue

                        img_elem = td_elements[0].find("img")
                        product_code = td_elements[1].text.strip()
                        power = td_elements[2].text.strip()
                        input_voltage = td_elements[3].text.strip()
                        output_voltage = td_elements[4].text.strip()
                        current = td_elements[5].text.strip()

                        datasheet_elem = row.find(
                            lambda tag: tag.name == "a" and "Datasheet" in tag.text
                        )

                        product = {
                            "image": img_elem["src"]
                            if img_elem and img_elem.has_attr("src")
                            else "",
                            "productCode": product_code,
                            "power": power,
                            "inputVoltage": input_voltage,
                            "outputVoltage": output_voltage,
                            "outputCurrent": current,
                            "datasheet": datasheet_elem["href"]
                            if datasheet_elem and datasheet_elem.has_attr("href")
                            else "",
                            "series_url": series_url,
                            "common_title": description.get("title", ""),
                            "common_subtitle": description.get("subtitle", ""),
                            "common_description": description.get("description", ""),
                            "common_features": "\n".join(features),
                        }

                        all_products.append(product)
                    except Exception as e:
                        logging.warning(f"Error processing product row: {str(e)}")
                        continue

                # Add delay between series
                await asyncio.sleep(2)
            except Exception as e:
                logging.error(
                    f"Error scraping series {series.get('productCode', 'unknown')}: {str(e)}"
                )
                continue

        await browser.close()

    return all_products


def main(params) -> dict:
    """Activity function triggered by the orchestrator"""
    logging.info("Scrape products function processing a request.")

    try:
        # Get parameters from request
        manufacturer = params.get("manufacturer", "recom")
        product_type = params.get("product_type", "dc-dc-converters")

        # Initialize environment
        env = AzureEnvironment()

        # Scrape products based on manufacturer
        products = []
        if manufacturer == "recom":
            products = asyncio.run(scrape_recom_products(env, product_type))
        elif manufacturer == "traco":
            products = asyncio.run(scrape_traco_products(env, product_type))
        elif manufacturer == "xppower":
            products = asyncio.run(scrape_xppower_products(env, product_type))
        else:
            raise ValueError(f"Unsupported manufacturer: {manufacturer}")

        # Convert to DataFrame
        df = pd.DataFrame(products)

        # Save to storage
        step_name = f"{manufacturer}2_scrape_products"
        file_name = f"{product_type}.csv"
        env.storage.save_df(step_name, file_name, df)

        return {
            "success": True,
            "manufacturer": manufacturer,
            "product_type": product_type,
            "count": len(products),
            "step_name": step_name,
            "file_name": file_name,
        }
    except Exception as e:
        logging.error(f"Error in scrape_products: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "manufacturer": params.get("manufacturer", "recom"),
            "product_type": params.get("product_type", "dc-dc-converters"),
        }
