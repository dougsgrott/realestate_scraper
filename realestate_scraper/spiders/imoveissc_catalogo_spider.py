from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import Spider, signals
from scrapy import Request
from w3lib.html import remove_tags

from itemloaders.processors import TakeFirst
from datetime import datetime
import sys

import time
import random
import logging
#from scrapy.utils.log import configure_logging 
#from scrapy import logformatter

sys.path.append("/home/user/PythonProj/Scraping/realestate_scraper/realestate_scraper")
from items import ImoveisSCCatalogItem


class ImoveisSCCatalogSpider(Spider):
    name = 'imoveis_sc_catalog'
    handle_httpstatus_list = [404]
    # start_urls = ['https://www.imoveis-sc.com.br/regiao-serra/']
    # start_urls = ['https://www.imoveis-sc.com.br/sao-bento-do-sul/comprar/casa']
    start_urls = ['https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa']  # LEVEL 1
    # start_urls = [
    #     'http://www.example.com/thisurlexists.html',
    #     'http://www.example.com/thisurldoesnotexist.html',
    #     'http://www.example.com/neitherdoesthisone.html'
    # ]
    # configure_logging(install_root_handler=False)
    # logging.basicConfig(
    #     filename='log.txt',
    #     format='%(levelname)s: %(message)s',
    #     level=logging.WARNING
    # )

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.SaveImoveisSCCatalogPipeline': 200,
            'realestate_scraper.pipelines.DuplicatesImoveisSCCatalogPipeline': 100,
            'realestate_scraper.pipelines.LoggerImoveisSCCatalogPipeline': 500,
        },
        'LOG_FORMATTER': 'realestate_scraper.middlewares.PoliteLogFormatter',
        'LOG_LEVEL': 'INFO',
        'LOG_FORMAT': '[%(name)s] %(levelname)s: %(message)s',
        'LOG_ENABLED': False,
        'LOG_FILE': 'log.txt',
    }    

    def __init__(self, *args, **kwargs):
        # logger = logging.getLogger(__name__)  # Gets or creates a logger
        # logger.info("================================")
        logger = logging.getLogger('scrapy.middleware')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.extensions.telnet')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.extensions.logstats')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.extensions.throttle')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.core.engine')
        logger.setLevel(logging.WARNING)

        logger = logging.getLogger('scrapy.downloadermiddlewares.retry')
        logger.setLevel(logging.CRITICAL)
        logger = logging.getLogger('scrapy.core.scraper')
        logger.setLevel(logging.CRITICAL)
        
        # super().__init__(*args, **kwargs)
        # self.failed_urls = []

        # super(ImoveisSCCatalogSpider, self).__init__(*args, **kwargs)
        # self.start_urls = [kwargs.get('start_url')]

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     print("\nfrom_crawler\n")
    #     spider = super(ImoveisSCCatalogSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
    #     return spider

    # def log_stats(self, stats):
    #     self.logger.info('teste - from spider')

    # def handle_spider_closed(self, reason):
    #     print("\nhandle_spider_closed\n")
    #     self.crawler.stats.set_value('failed_urls', ', '.join(self.failed_urls))
    #     #i = 10

    # def process_exception(self, response, exception, spider):
    #     print("\nprocess_exception\n")
    #     ex_class = "%s.%s" % (exception.__class__.__module__, exception.__class__.__name__)
        
    #     self.crawler.stats.inc_value('failed_url_count')
    #     self.failed_urls.append(response.url)
    #     print(response.status)

    #     self.crawler.stats.inc_value('downloader/exception_count', spider=spider)
    #     self.crawler.stats.inc_value('downloader/exception_type_count/%s' % ex_class, spider=spider)

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        # print("\nparse\n")
        # if response.status == 404:
        #     self.crawler.stats.inc_value('failed_url_count')
        #     self.failed_urls.append(response.url)
        i = 0
        for sel in response.xpath('//article[@class="imovel  "]'):
            yield self.populate_item(sel, response.url)

        time.sleep(random.randint(3,7))
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 1
    def populate_item(self, selector, url):
        item_loader = ItemLoader(item=ImoveisSCCatalogItem(), selector=selector)
        item_loader.default_output_processor = TakeFirst()
        item_loader.add_xpath('title', './/h2[@class="imovel-titulo"]/a/meta[@itemprop="name"]/@content')
        item_loader.add_xpath('code', './/div[@class="imovel-extra"]/span/text()')
        item_loader.add_xpath('local', './/div[@class="imovel-extra"]/strong/text()')
        item_loader.add_xpath('description', './/p[@class="imovel-descricao"]/text()')
        item_loader.add_value('region', url)
        item_loader.add_xpath('url', './/a[contains(@class, "btn-visualizar")]/@href')
        item_loader.add_value('date', datetime.now()) #.isoformat(' ')
        foo = item_loader.load_item()
        return item_loader.load_item()
    

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(ImoveisSCCatalogSpider)
    process.start()
    


# settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.signals.connect(callback, signal=signals.spider_closed)
#     crawler.configure()
#     crawler.crawl(spider)
#     crawler.start()
#     log.start()
#     reactor.run()