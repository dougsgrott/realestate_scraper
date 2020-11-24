from scrapy.crawler import CrawlerProcess

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import Spider
from scrapy import Request
from w3lib.html import remove_tags

from itemloaders.processors import TakeFirst
from datetime import datetime
import sys

sys.path.append("/home/user/PythonProj/Scraping/realestate_scraper/realestate_scraper")
from items import ImoveisSCCatalogItem

class ImoveisSCCatalogSpider(Spider):
    name = 'imoveis_sc_catalog'
    start_urls = ['https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa']  # LEVEL 1
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
    }

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        i = 0
        for sel in response.xpath('//article[@class="imovel  "]'):
            yield self.populate_item(sel)

        yield self.paginate(response)


    # 2. SCRAPING LEVEL 1
    def populate_item(self, selector):
        item_loader = ItemLoader(item=ImoveisSCCatalogItem(), selector=selector)
        item_loader.default_output_processor = TakeFirst()
        #item_loader.add_xpath('title', '//h2[@class="imovel-titulo"]/a/text()')
        item_loader.add_xpath('title', '//h2[@class="imovel-titulo"]/a/meta[@itemprop="name"]/@content')
        item_loader.add_xpath('code', '//div[@class="imovel-extra"]/span/text()')
        item_loader.add_xpath('local', '//div[@class="imovel-extra"]/strong/text()')
        item_loader.add_xpath('description', '//p[@class="imovel-descricao"]/text()')
        item_loader.add_xpath('url', '//a[contains(@class, "btn-visualizar")]/@href')
        item_loader.add_value('date_scraped', datetime.now().isoformat(' '))

        return item_loader.load_item()
    

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


process = CrawlerProcess()
process.crawl(ImoveisSCCatalogSpider)
process.start()
