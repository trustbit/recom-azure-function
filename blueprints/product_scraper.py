import logging
import asyncio
import azure.functions as func
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from shared.environment import AzureEnvironment

# Create blueprint instance
bp = func.Blueprint()


# --------------- Scrape Products Activity Function ---------------
@bp.activity_trigger(input_name="input")
def scrape_products(input: dict) -> dict:
    """Activity function to scrape detailed product data"""
    # logging.info(
    #     f"Scraping products for {input['manufacturer']} {input['product_type']}"
    # )
    #
    # try:
    #     # Get parameters
    #     manufacturer = input.get("manufacturer", "recom")
    #     product_type = input.get("product_type", "dc-dc-converters")
    #
    #     # Initialize environment
    #     env = AzureEnvironment()
    #
    #     # Choose scraper based on manufacturer
    #     if manufacturer == "recom":
    #         products_data = asyncio.run(scrape_recom_products(env, product_type))
    #     elif manufacturer == "traco":
    #         products_data = asyncio.run(scrape_traco_products(env, product_type))
    #     elif manufacturer == "xppower":
    #         products_data = asyncio.run(scrape_xppower_products(env, product_type))
    #     else:
    #         raise ValueError(f"Unsupported manufacturer: {manufacturer}")
    #
    #     # Convert to DataFrame and save
    #     df = pd.DataFrame(products_data)
    #
    #     step_name = f"{manufacturer}2_scrape_products"
    #     file_name = f"{product_type}.csv"
    #     env.storage.save_df(step_name, file_name, df)
    #
    #     return {
    #         "success": True,
    #         "manufacturer": manufacturer,
    #         "product_type": product_type,
    #         "count": len(products_data),
    #         "step_name": step_name,
    #         "file_name": file_name,
    #     }
    # except Exception as e:
    #     logging.error(f"Error in scrape_products: {str(e)}")
    #     return {
    #         "success": False,
    #         "error": str(e),
    #         "manufacturer": input.get("manufacturer", "recom"),
    #         "product_type": input.get("product_type", "dc-dc-converters"),
    #     }


async def scrape_recom_products(env, product_type):
    """Scrape RECOM products from series data"""
    # Load series data
    # step_name = "recom1_scrape_series"
    # file_name = f"{product_type}.csv"
    # series_df = env.storage.load_df(step_name, file_name)
    #
    # all_products = []
    # async with async_playwright() as p:
    #     browser = await p.chromium.launch(headless=True)
    #
    #     for _, series in series_df.iterrows():
    #         page = await browser.new_page()
    #         try:
    #             # Navigate to series page
    #             url = series["product_link"]
    #             await page.goto(url, wait_until="networkidle")
    #
    #             # Wait for products table
    #             await page.wait_for_selector("table.productTable", timeout=10000)
    #
    #             # Get all product rows
    #             rows = await page.query_selector_all("table.productTable tbody tr")
    #
    #             series_products = []
    #             for row in rows:
    #                 try:
    #                     # Extract product data
    #                     cells = await row.query_selector_all("td")
    #
    #                     if len(cells) < 5:  # Skip header or invalid rows
    #                         continue
    #
    #                     product_code = await cells[0].inner_text()
    #                     description = await cells[1].inner_text()
    #                     input_voltage = await cells[2].inner_text()
    #                     output_voltage = await cells[3].inner_text()
    #                     current = await cells[4].inner_text()
    #
    #                     # Get datasheet link
    #                     datasheet_cell = await row.query_selector(
    #                         "td.additional-column"
    #                     )
    #                     datasheet_link = ""
    #                     if datasheet_cell:
    #                         link_elem = await datasheet_cell.query_selector("a.btn")
    #                         if link_elem:
    #                             datasheet_link = await link_elem.get_attribute("href")
    #
    #                     product = {
    #                         "series_name": series["product_name"],
    #                         "product_code": product_code,
    #                         "description": description,
    #                         "input_voltage": input_voltage,
    #                         "output_voltage": output_voltage,
    #                         "current": current,
    #                         "datasheet_link": datasheet_link,
    #                         "series_power": series["power"],
    #                         "series_mounting_type": series["mounting_type"],
    #                         "series_package_style": series["package_style"],
    #                     }
    #
    #                     series_products.append(product)
    #                 except Exception as e:
    #                     logging.warning(f"Error processing product row: {str(e)}")
    #                     continue
    #
    #             all_products.extend(series_products)
    #             logging.info(
    #                 f"Scraped {len(series_products)} products from {series['product_name']}"
    #             )
    #
    #         except Exception as e:
    #             logging.error(
    #                 f"Error scraping series {series.get('product_name', 'unknown')}: {str(e)}"
    #             )
    #         finally:
    #             await page.close()
    #
    #     await browser.close()
    #
    # return all_products


async def scrape_traco_products(env, product_type):
    """Scrape Traco products from series data"""
    # Load series data
    step_name = "traco1_scrape_series"
    file_name = f"{product_type}.csv"
    series_df = env.storage.load_df(step_name, file_name)

    all_products = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for _, series in series_df.iterrows():
            if not series.get("series_url"):
                continue

            page = await browser.new_page()
            try:
                # Navigate to series page
                url = series["series_url"]
                await page.goto(url, wait_until="networkidle")

                # Get HTML content
                html_content = await page.content()
                soup = BeautifulSoup(html_content, "html.parser")

                # Find the products table
                product_table = soup.select_one("table.models")
                if not product_table:
                    continue

                # Get all product rows
                rows = product_table.select("tbody tr")

                series_products = []
                for row in rows:
                    try:
                        cells = row.select("td")
                        if len(cells) < 5:
                            continue

                        part_number = cells[0].select_one("a")
                        part_number_text = (
                            part_number.text.strip() if part_number else ""
                        )

                        input_voltage = cells[1].text.strip() if len(cells) > 1 else ""
                        output_voltage = cells[2].text.strip() if len(cells) > 2 else ""
                        power = cells[3].text.strip() if len(cells) > 3 else ""
                        efficiency = cells[4].text.strip() if len(cells) > 4 else ""

                        datasheet_link = ""
                        datasheets = soup.select(".field--name-field-datasheets a")
                        if datasheets:
                            datasheet_link = (
                                "https://www.tracopower.com" + datasheets[0]["href"]
                            )

                        product = {
                            "series_name": series.get("product_series", ""),
                            "product_code": part_number_text,
                            "input_voltage": input_voltage,
                            "output_voltage": output_voltage,
                            "power": power,
                            "efficiency": efficiency,
                            "datasheet_link": datasheet_link,
                            "series_power_variants": series.get(
                                "series_power_variants", ""
                            ),
                            "series_input_voltage_variants": series.get(
                                "series_input_voltage_variants", ""
                            ),
                            "series_output_voltage_variants": series.get(
                                "series_output_voltage_variants", ""
                            ),
                            "series_description": series.get(
                                "series_short_description", ""
                            ),
                        }

                        series_products.append(product)
                    except Exception as e:
                        logging.warning(f"Error processing Traco product row: {str(e)}")
                        continue

                all_products.extend(series_products)
                logging.info(
                    f"Scraped {len(series_products)} products from {series.get('product_series', 'unknown')}"
                )

            except Exception as e:
                logging.error(
                    f"Error scraping Traco series {series.get('product_series', 'unknown')}: {str(e)}"
                )
            finally:
                await page.close()

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

        for _, series in series_df.iterrows():
            if not series.get("url"):
                continue

            page = await browser.new_page()
            try:
                # Navigate to series page
                url = (
                    "https://www.xppower.com" + series["url"]
                    if not series["url"].startswith("http")
                    else series["url"]
                )
                await page.goto(url, wait_until="networkidle")

                # Wait for products table
                await page.wait_for_selector("table.variations-table", timeout=10000)

                # Get HTML content
                html_content = await page.content()
                soup = BeautifulSoup(html_content, "html.parser")

                # Find the products table
                product_table = soup.select_one("table.variations-table")
                if not product_table:
                    continue

                # Get all product rows
                rows = product_table.select("tbody tr")

                series_products = []
                for row in rows:
                    try:
                        cells = row.select("td")
                        if len(cells) < 3:
                            continue

                        model = cells[0].text.strip() if len(cells) > 0 else ""
                        input_voltage = cells[1].text.strip() if len(cells) > 1 else ""
                        output_voltage = cells[2].text.strip() if len(cells) > 2 else ""

                        # Use series datasheet for all products
                        datasheet_link = series.get("datasheet", "")
                        if datasheet_link and not datasheet_link.startswith("http"):
                            datasheet_link = "https://www.xppower.com" + datasheet_link

                        product = {
                            "series_name": series.get("productCode", ""),
                            "product_code": model,
                            "input_voltage": input_voltage,
                            "output_voltage": output_voltage,
                            "power": series.get("power", ""),
                            "phase": series.get("phase", ""),
                            "voltage": series.get("voltage", ""),
                            "current": series.get("current", ""),
                            "datasheet_link": datasheet_link,
                        }

                        series_products.append(product)
                    except Exception as e:
                        logging.warning(
                            f"Error processing XP Power product row: {str(e)}"
                        )
                        continue

                all_products.extend(series_products)
                logging.info(
                    f"Scraped {len(series_products)} products from {series.get('productCode', 'unknown')}"
                )

            except Exception as e:
                logging.error(
                    f"Error scraping XP Power series {series.get('productCode', 'unknown')}: {str(e)}"
                )
            finally:
                await page.close()

        await browser.close()

    return all_products
