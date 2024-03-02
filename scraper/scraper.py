import asyncio
import logging
import random
from datetime import datetime
from urllib.parse import urlparse

from azure.storage.blob.aio import BlobServiceClient
from playwright.async_api import async_playwright, TimeoutError

class AsyncWebScraper:
    def __init__(self, urls, connection_string, container_name, proxy_base, proxy_ports_range=(10000, 11000)):
        self.urls = urls
        self.connection_string = connection_string
        self.container_name = container_name
        self.proxy_base = proxy_base
        self.proxy_ports = list(range(*proxy_ports_range))
        self.proxy_port_lock = asyncio.Lock()

    async def select_proxy_port(self):
        async with self.proxy_port_lock:
            if self.proxy_ports:
                port = self.proxy_ports.pop(random.randint(0, len(self.proxy_ports) - 1))
                logging.info(f"Selected proxy port: {port}")
                return port
            else:
                logging.error("No more unique proxy ports available.")
                return None

    async def release_proxy_port(self, port):
        async with self.proxy_port_lock:
            self.proxy_ports.append(port)
            logging.info(f"Released proxy port: {port}")

    async def browse_with_proxy_and_save(self, url):
        port = await self.select_proxy_port()
        if port is None:
            logging.error(f"Failed to obtain a proxy port for URL: {url}")
            return

        proxy_server = f"http://{self.proxy_base}:{port}"
        logging.info(f"Using proxy server: {proxy_server} for URL: {url}")
        retries = 0
        max_retries = 5

        while retries < max_retries:
            try:
                async with async_playwright() as p:
                    browser = await p.firefox.launch(proxy={'server': proxy_server})
                    page = await browser.new_page()
                    logging.info(f"Going to URL: {url}")
                    await page.goto(url, timeout=60000)
                    inner_text = await page.inner_text('body')
                    logging.info(f"Successfully fetched URL: {url}")
                    await self.save_page_data(inner_text, urlparse(url).netloc)
                    break
            except TimeoutError:
                logging.warning(f"TimeoutError for {url} with proxy {proxy_server}. Retrying...")
                retries += 1
            except Exception as e:
                logging.error(f"Error fetching {url} with proxy {proxy_server}: {e}")
                retries += 1
            finally:
                await browser.close()

        if retries >= max_retries:
            logging.error(f"Failed to fetch {url} after {max_retries} retries with proxy {proxy_server}.")

    async def save_page_data(self, data, domain):
        logging.info(f"Saving data for domain: {domain}")
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_service_client.get_container_client(self.container_name)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        blob_name = f"{domain}-{timestamp}.txt"
        blob_client = container_client.get_blob_client(blob=blob_name)
        await blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Successfully saved data for domain: {domain}")

    async def run(self):
        logging.info("Starting web scraping tasks.")
        tasks = [self.browse_with_proxy_and_save(url) for url in self.urls]
        await asyncio.gather(*tasks)
        logging.info("Completed web scraping tasks.")
