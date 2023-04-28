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
sys.path.append("/media/user/Novo volume/Python/Secondary/realestate_scraper/realestate_scraper")
from items import VivaRealCatalogItem#, VivaRealStatusItem
import settings


class VivaRealCatalogSpider(Spider):
    name = 'vivareal_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30
    # start_urls = ['https://www.imoveis-sc.com.br/regiao-serra/']
    # start_urls = ['https://www.imoveis-sc.com.br/sao-bento-do-sul/comprar/casa']
    # start_urls = ['https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa?ordenacao=recentes&page=1']  # LEVEL 1
    start_urls = [
        # 'https://www.imoveis-sc.com.br/regiao-oeste/alugar/casa',          
        # 'https://www.imoveis-sc.com.br/regiao-norte/',
        'https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial',
        ]
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
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            # 'realestate_scraper.pipelines.DuplicatesImoveisSCCatalogPipeline': 100,
            'realestate_scraper.pipelines.SaveVivaRealCatalogPipeline': 200,
            'realestate_scraper.pipelines.JsonWriterPipeline': 300,
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(VivaRealCatalogSpider, cls).from_crawler(crawler, *args, **kwargs)
    # def from_crawler(self, cls, crawler, *args, **kwargs):
    #     spider = super(self, cls).from_crawler(crawler, *args, **kwargs)
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
        for sel in response.xpath('//*[contains(@class, "results-list")]/div'):
            yield self.populate_catalog(sel, response.url)
            
        print(f"redundancy_threshold: {self.redundancy_threshold}")
        print(f"redundancy_streak: {settings.redundancy_streak}")
        if (settings.redundancy_streak > self.redundancy_threshold):
            reason = "Reason: more than {} consecutive redundant entries.".format(self.redundancy_threshold)
            raise CloseSpider(reason=reason)

        time.sleep(random.randint(3,7))
        # yield self.paginate(response)

# address = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[@class="property-card__address"]/text()').extract()[0] 
# title = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[@class="property-card__title js-cardLink js-card-title"]/text()').extract()[0] 
# details = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[contains(@class, "property-card__detail-item")]').extract()
# amenities = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[contains(@class, "property-card__amenities")]').extract()
# values = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[contains(@class, "property-card__values")]').extract()
# link = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[contains(@class, "property-card__carousel")]/a/@href').extract()
# link = response.xpath('//*[@class="results-list js-results-list"]/div')[0].xpath('.//*[contains(@class, "property-card__container")]/a/@href').extract() 

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
        # next_page_url = response.xpath('//a[@class="next"]/@href').get()
        next_page_url = response.xpath('//*[contains(@class, "js-change-page")]')[-1]
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(VivaRealCatalogSpider)
    process.start()


# response.xpath('//*[@class="results-list js-results-list"]/*').extract()[0]


