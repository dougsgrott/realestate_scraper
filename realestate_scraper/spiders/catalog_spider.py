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

import sys
sys.path.append("/home/user/PythonProj/realestate_scraper/realestate_scraper")
from items import ImoveisSCCatalogItem, ImoveisSCStatusItem
import settings


class ImoveisSCCatalogSpider(Spider):
    name = 'imoveis_sc_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30
    # start_urls = ['https://www.imoveis-sc.com.br/regiao-serra/']
    # start_urls = ['https://www.imoveis-sc.com.br/sao-bento-do-sul/comprar/casa']
    # start_urls = ['https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa?ordenacao=recentes&page=1']  # LEVEL 1
    start_urls = ['https://www.imoveis-sc.com.br/regiao-oeste/alugar/casa',
                  'https://www.imoveis-sc.com.br/regiao-norte/']
    # start_urls = [
    #     'http://www.example.com/thisurlexists.html',
    #     'http://www.example.com/thisurldoesnotexist.html',
    #     'http://www.example.com/neitherdoesthisone.html'
    # ]

    customLogger = logging.getLogger(__name__)
    customLogger.setLevel(logging.INFO)
    file_handler = logging.FileHandler('logfile.txt')
    formatter = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
    file_handler.setFormatter(formatter)
    customLogger.addHandler(file_handler)

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.DuplicatesImoveisSCCatalogPipeline': 100,
            'realestate_scraper.pipelines.SaveImoveisSCCatalogPipeline': 200,
        },
    }    

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ImoveisSCCatalogSpider, cls).from_crawler(crawler, *args, **kwargs)
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
        for sel in response.xpath('//article[@class="imovel  "]'):
            yield self.populate_catalog(sel, response.url)
            
        print(self.redundancy_threshold)
        print(settings.redundancy_streak)
        if (settings.redundancy_streak > self.redundancy_threshold):
            reason = "Reason: more than {} consecutive redundant entries.".format(self.redundancy_threshold)
            raise CloseSpider(reason=reason)

        time.sleep(random.randint(3,7))
        yield self.paginate(response)

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
        return loaded_item
    
    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(ImoveisSCCatalogSpider)
    process.start()
    