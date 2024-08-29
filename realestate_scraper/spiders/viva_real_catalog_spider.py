import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scrapy.exceptions import CloseSpider
from scrapy.loader import ItemLoader
from scrapy.spiders import Spider, signals

from itemloaders.processors import TakeFirst
from datetime import datetime
from scrapy_playwright.page import PageMethod
import time
import random
import logging
import pprint

import sys
import os
curr_path = os.path.dirname(os.path.realpath(__file__))
base_path = os.path.abspath(os.path.join(curr_path, os.pardir))
sys.path.append(base_path)
from items import VivaRealCatalogItem
import settings

from scrapy.utils.reactor import install_reactor

from playwright.sync_api import sync_playwright, expect
import time
import logging

# from twisted.internet import reactor, defer
# from scrapy.crawler import CrawlerRunner
# from scrapy.utils.log import configure_logging


class VivaRealCatalogSpider(Spider):
    name = 'vivareal_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30
    install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

    customLogger = logging.getLogger(__name__)
    customLogger.setLevel(logging.INFO)
    file_handler = logging.FileHandler('logfile.txt')
    formatter = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
    file_handler.setFormatter(formatter)
    customLogger.addHandler(file_handler)

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            # 'realestate_scraper.pipelines.DuplicatesImoveisSCCatalogPipeline': 100,
            # 'realestate_scraper.pipelines.SaveVivaRealCatalogPipeline': 200,
            # 'realestate_scraper.pipelines.JsonWriterPipeline': 300,
            'pipelines.JsonWriterPipeline': 300,
        },
    }

    def start_requests(self):
        url = "https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial"
        yield scrapy.Request(
            url,
            callback=self.parse,
            meta={
                'playwright': True,
                'playwright_include_page': True,
                # 'playwright_page_methods': [PageMethod('wait_for_selector', 'button.js-change-page')],
                'errback':self.errback,
            }
        )

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(VivaRealCatalogSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        crawler.signals.connect(spider.handle_spider_opened, signals.spider_opened)
        return spider

    def handle_spider_opened(self):
        self.customLogger.info("Spider Opened")

    def log_stats(self, stats):
        self.customLogger.info("Scraping Stats:\n" + pprint.pformat(stats))

    def handle_spider_closed(self, reason=""):
        print("Reason: {}".format(reason))
        stats = self.crawler.stats.get_stats()
        self.log_stats(stats)
        self.customLogger.info("Spider Closed. {}".format(reason))

    # 1. FOLLOWING LEVEL 1
    async def parse(self, response):
        page = response.meta["playwright_page"]
        # await page.click("button.js-change-page")


        # # Extract HTML content
        # html_content = await page.content()
        
        # # Define the file path
        # file_path = os.path.join("scraped_pages", f"page_{int(time.time())}.html")
        
        # # Ensure the directory exists
        # os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # # Write the HTML content to a file
        # with open(file_path, "w", encoding="utf-8") as file:
        #     file.write(html_content)



        for sel in response.xpath('//*[contains(@class, "results-list")]/div'):
            yield self.populate_catalog(sel, response.url)

        print(f"redundancy_threshold: {self.redundancy_threshold}")
        print(f"redundancy_streak: {settings.redundancy_streak}")
        if (settings.redundancy_streak > self.redundancy_threshold):
            reason = "Reason: more than {} consecutive redundant entries.".format(self.redundancy_threshold)
            raise CloseSpider(reason=reason)

        time.sleep(random.randint(3,7))

    # 2. SCRAPING LEVEL 1
    def populate_catalog(self, selector, url):
        catalog_loader = ItemLoader(item=VivaRealCatalogItem(), selector=selector)
        catalog_loader.default_output_processor = TakeFirst()
        catalog_loader.add_value('type', "catalog")
        catalog_loader.add_xpath('address', './/*[@class="property-card__address"]/text()')
        catalog_loader.add_xpath('title', './/*[contains(@class, "property-card__title")]/text()')
        catalog_loader.add_xpath('details', './/*[contains(@class, "property-card__detail-item")]')
        catalog_loader.add_xpath('amenities', './/*[contains(@class, "property-card__amenities")]')
        catalog_loader.add_xpath('values', './/*[contains(@class, "property-card__values")]')
        catalog_loader.add_xpath('target_url', './/*[contains(@class, "property-card__carousel")]/a/@href')
        catalog_loader.add_value('catalog_scraped_date', datetime.now().isoformat())
        catalog_loader.add_value('is_target_scraped', 0)
        loaded_item = catalog_loader.load_item()
        return loaded_item

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        pass




if __name__ == '__main__':

    url = 'https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial'

    # scrape_url(url)
    process = CrawlerProcess(get_project_settings())
    process.crawl(VivaRealCatalogSpider, start_urls=[url])
    process.start()
    print("EOL")
