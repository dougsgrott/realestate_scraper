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
sys.path.append("/media/user/Novo volume/Python/Secondary/realestate_scraper/realestate_scraper")
from items import VivaRealCatalogItem#, VivaRealStatusItem
import settings


class VivaRealCatalogSpider(Spider):
    name = 'vivareal_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30
    # start_urls = [
    #     'https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial',
    #     ]

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
            'realestate_scraper.pipelines.SaveVivaRealCatalogPipeline': 200,
            # 'realestate_scraper.pipelines.JsonWriterPipeline': 300,
        },
    }

    def start_requests(self):
        url = "https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial"
        yield scrapy.Request(
            url,
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_page_methods': [PageMethod('wait_for_selector', 'button.js-change-page')],
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
    def parse(self, response):
        page = response.meta["playwright_page"]

        # locator = page.locator("text=Próxima página")
        # is_locator_enabled = locator.get_attribute('data-disabled') == None
        # await page.close()


        for sel in response.xpath('//*[contains(@class, "results-list")]/div'):
            yield self.populate_catalog(sel, response.url)

        print(f"redundancy_threshold: {self.redundancy_threshold}")
        print(f"redundancy_streak: {settings.redundancy_streak}")
        if (settings.redundancy_streak > self.redundancy_threshold):
            reason = "Reason: more than {} consecutive redundant entries.".format(self.redundancy_threshold)
            raise CloseSpider(reason=reason)

        time.sleep(random.randint(3,7))
        # yield self.paginate(response)

# response.xpath('/html/body/main/div[2]/div[1]/section/div[2]/div[2]/div/ul/li[6]/button').extract()[0]


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
        # catalog_loader.add_value('catalog_scraped_date', datetime.now())
        catalog_loader.add_value('catalog_scraped_date', datetime.now().isoformat())
        catalog_loader.add_value('is_target_scraped', 0)
        loaded_item = catalog_loader.load_item()
        return loaded_item

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        pass
        # next_page_url = response.xpath('//a[@class="next"]/@href').get()
        # next_page_url = response.xpath('//*[contains(@class, "js-change-page")]')[-1]
        # if next_page_url is not None:
        #     return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    from playwright.sync_api import sync_playwright, expect
    import time
    import logging

    process = CrawlerProcess(get_project_settings())
    process.crawl(VivaRealCatalogSpider)
    process.start()

    # LOGGER = logging.getLogger(__name__)

    # with sync_playwright() as p:
    #     navigator = p.chromium.launch(headless=False)
    #     page = navigator.new_page()
    #     page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")

    #     locator = page.locator("text=Próxima página")
    #     is_locator_enabled = locator.get_attribute('data-disabled') == None

    #     while is_locator_enabled:
    #         locator.click()
    #         time.sleep(3)
    #         locator = page.locator("text=Próxima página")
    #         is_locator_enabled = locator.get_attribute('data-disabled') == None

    #     foo = 42

    foo = 42


# response.xpath('//*[@class="results-list js-results-list"]/*').extract()[0]


