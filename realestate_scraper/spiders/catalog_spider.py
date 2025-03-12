from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scrapy.exceptions import CloseSpider
from scrapy.loader import ItemLoader
from scrapy.spiders import Spider, signals

from itemloaders.processors import TakeFirst
from datetime import datetime

import time
import random
import logging
import pprint

from sqlalchemy.orm import sessionmaker

import sys
import os

sys.path.append("/mnt/FE86DAF186DAAA03/Python/Secondary/realestate_scraper/realestate_scraper")
from items import ImoveisSCCatalogItem, ImoveisSCStatusItem
import settings

from models import db_connect, ImoveisSCCatalog
from planners import BasicSkipper


class ImoveisSCCatalogSpider(Spider):
    name = 'imoveis_sc_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30

    log_directory = 'logs'
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, 'imoveis_sc_catalog_spider.log')
    customLogger = logging.getLogger(__name__)
    customLogger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
    file_handler.setFormatter(formatter)
    customLogger.addHandler(file_handler)

    custom_settings = {
        # 'LOG_FILE': 'imoveis_sc_catalog_spider.log',
        # 'LOG_LEVEL': 'INFO',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.DuplicatesImoveisSCCatalogPipeline': 100,
            'realestate_scraper.pipelines.SaveImoveisSCCatalogPipeline': 200,
        },
    }

    def __init__(self, start_urls, close_due_to_redundancy=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls
        self.close_due_to_redundancy = close_due_to_redundancy
        self.page_items = []
        self.duplicated_page_count = 0
        self.skipping = False
        self.planner = BasicSkipper(threshold=3, skip_n=10)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ImoveisSCCatalogSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        crawler.signals.connect(spider.handle_spider_opened, signals.spider_opened)
        return spider

    def handle_spider_opened(self):
        self.customLogger.info(f"Spider opened: {self.name}")

    def log_stats(self, stats):
        self.customLogger.info("Scraping Stats:\n" + pprint.pformat(stats))

    def handle_spider_closed(self, reason=""):
        print("Reason: {}".format(reason))
        stats = self.crawler.stats.get_stats()
        self.log_stats(stats)
        self.customLogger.info(f"Spider Closed. {reason}")

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        self.page_items = []
        selector = '//article[@class="imovel  "]'
        
        new_page = self.planner.foo(response, selector, self.parse)
        time.sleep(random.randint(3,7))

        for sel in response.xpath(selector):
            yield self.populate_catalog(sel, response.url)

        # print(f"REDUNDANCY - Threshold: {self.redundancy_threshold}, Streak: {settings.redundancy_streak}")
        self.customLogger.info("REDUNDANCY - Threshold: %d, Streak: %d",
                         self.redundancy_threshold, settings.redundancy_streak)

        if self.close_due_to_redundancy:
            if (settings.redundancy_streak > self.redundancy_threshold):
                reason = "Reason: more than {} consecutive redundant entries.".format(self.redundancy_threshold)
                self.customLogger.warning("Closing spider due to redundancy: %s", reason)
                raise CloseSpider(reason=reason)

        # Resumer().get_attribute_from_selectors(response, selector)

        # if self.close_due_to_redundancy:
        #     if (settings.redundancy_streak > self.redundancy_threshold):
        # self.foo(response, selector)
        if new_page == None:
            raise CloseSpider()
        yield response.follow(new_page, self.parse)

        # yield self.paginate(response)

    # 2. SCRAPING LEVEL 1
    def populate_catalog(self, selector, url):
        catalog_loader = ItemLoader(item=ImoveisSCCatalogItem(), selector=selector)
        catalog_loader.default_output_processor = TakeFirst()
        catalog_loader.add_value('type', "catalog")
        catalog_loader.add_xpath('title', './/h2[@class="imovel-titulo"]/a/meta[@itemprop="name"]/@content')
        catalog_loader.add_xpath('code', './/div[@class="imovel-extra"]/span/text()')
        catalog_loader.add_xpath('local', './/div[@class="imovel-extra"]/strong/text()')
        catalog_loader.add_xpath('description', './/p[@class="imovel-descricao"]/text()')
        catalog_loader.add_value('region', url)
        catalog_loader.add_value('scraped_date', datetime.now()) #.isoformat(' ')
        catalog_loader.add_xpath('url', './/a[contains(@class, "btn-visualizar")]/@href')
        catalog_loader.add_value('url_is_scraped', 0)
        loaded_item = catalog_loader.load_item()
        self.page_items.append(loaded_item)
        return loaded_item

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(ImoveisSCCatalogSpider, start_urls=['https://www.imoveis-sc.com.br/regiao-serra/'])
    process.start()

    # Possible start_urls:
    # https://www.imoveis-sc.com.br/joinville
    # https://www.imoveis-sc.com.br/regiao-serra/
    # https://www.imoveis-sc.com.br/florianopolis/
    # https://www.imoveis-sc.com.br/sao-bento-do-sul/comprar/casa
    # https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa?ordenacao=recentes&page=1
    # https://www.imoveis-sc.com.br/regiao-oeste/alugar/casa
    # https://www.imoveis-sc.com.br/regiao-norte/
    # https://www.imoveis-sc.com.br/regiao-oeste/