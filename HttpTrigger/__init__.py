import logging
import azure.functions as func
import asyncio
import os
from pathlib import Path
import sys

# Initial logging to capture the start of the function
logging.info("Azure Function is starting...")

# Log the current Python version and executable path
logging.info(f"Current Python version: {sys.version}")
logging.info(f"Python executable path: {sys.executable}")

# Ensure local modules can be imported by adjusting sys.path
function_app_dir = Path(__file__).parent.parent.absolute()
if str(function_app_dir) not in sys.path:
    sys.path.append(str(function_app_dir))
logging.info(f"Function app directory added to sys.path: {function_app_dir}")

# Log current sys.path
logging.info(f"Current sys.path: {sys.path}")

# Attempt to import the scraper with exception handling
try:
    from scraper.scraper import AsyncWebScraper
    logging.info("Successfully imported AsyncWebScraper.")
except ImportError as e:
    logging.error(f"Failed to import AsyncWebScraper: {e}. Ensure the 'scraper' module is correctly placed and named.")
    # Early return or raise an exception based on your handling strategy
    raise ImportError(f"Failed to import AsyncWebScraper: {e}.") from e

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Log incoming request details
    logging.info(f"Request method: {req.method}")
    logging.info(f"Request URL: {req.url}")
    logging.info(f"Request headers: {req.headers}")

    # Reading the URL from query or body
    url = req.params.get('url')
    if not url:
        try:
            req_body = await req.get_json()
            url = req_body.get('url')
        except ValueError as e:
            logging.error(f"JSON decoding failed: {e}")
        except Exception as e:
            logging.error(f"Unexpected error when reading request body: {e}")

    if not url:
        logging.error("No URL passed in request.")
        return func.HttpResponse("Please pass a URL on the query string or in the request body", status_code=400)

    # Log environment variables and configuration
    logging.info(f"AzureStorageConnectionString: {os.getenv('AzureStorageConnectionString')}")
    logging.info(f"CONTAINER_NAME: {os.getenv('CONTAINER_NAME', 'webcontent')}")
    logging.info(f"PROXY_BASE: {os.getenv('PROXY_BASE')}")
    logging.info(f"PROXY_PORTS_RANGE: {os.getenv('PROXY_PORTS_RANGE', '10000-11000')}")

    # Initialize the scraper with the provided details
    scraper = AsyncWebScraper([url], os.getenv("AzureStorageConnectionString"), os.getenv("CONTAINER_NAME", "webcontent"), os.getenv("PROXY_BASE"), map(int, os.getenv("PROXY_PORTS_RANGE", "10000-11000").split('-')))

    # Running the scraper
    loop = asyncio.get_event_loop()
    try:
        if loop.is_running():
            await scraper.run()
        else:
            loop.run_until_complete(scraper.run())
    except Exception as e:
        logging.error(f"Error while running scraper: {e}")

    scraped_content = "Scraping completed. Check the blob storage for results."
    return func.HttpResponse(scraped_content, status_code=200)
