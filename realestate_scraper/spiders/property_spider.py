#import scrapy
from scrapy.crawler import CrawlerProcess
#from datetime import datetime

#
# This file was created by Attila Toth - http://scrapingauthority.com
#
#
# This template is usable for TWO-LEVEL DEEP scrapers with pagination on the 1st level.
#
# HOW THE LOOP WORKS:
#
# 1. FOLLOWING LEVEL 1: Follow item urls.
# 2. SCRAPING LEVEL 2: Scrape the fields and populate item.
# 3. PAGINATION LEVEL 1: Go to the next page with the "next button" if any.
# 1. ...
#
#

from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.spiders import Spider
from scrapy import Request
from scrapy.utils.project import get_project_settings
from w3lib.html import remove_tags
from datetime import datetime
import sys
import time
import random
import logging


sys.path.append("/mnt/FE86DAF186DAAA03/Python/Secondary/realestate_scraper/realestate_scraper")
from items import ImoveisSCPropertyItem
from models import ImoveisSCCatalog, create_table, db_connect
from sqlalchemy.orm import sessionmaker

class ImoveisSCPropertySpider(Spider):
    name = 'imoveis_sc_properties'

    custom_settings = {
        # 'MONGODB_URI': 'mongodb://localhost:27017',
        # 'MONGODB_DATABASE': 'imoveis-sc',
        # 'MONGODB_COLLECTION': 'properties',
        # 'MONGODB_UNIQUE_KEY': 'url',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            # 'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
            # 'realestate_scraper.pipelines.MongoDBPipeline': 100,
            'realestate_scraper.pipelines.SaveImoveisSCPropertyPipeline': 100,
        }
    }

    def __init__(self, region=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region

    def start_requests(self):
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)

        with Session() as session:
            if self.region == None:
                rows_not_scraped = session.query(ImoveisSCCatalog).filter(ImoveisSCCatalog.url_is_scraped == 0).all()
            else:
                rows_not_scraped = session.query(ImoveisSCCatalog) \
                                        .filter(
                                            ImoveisSCCatalog.url_is_scraped == 0,
                                            ImoveisSCCatalog.region == self.region
                                        ).all()

            for row in rows_not_scraped:
                time.sleep(random.randint(3,7))
                yield Request(url=row.url, callback=self.parse, meta={'catalogo_id': row.id})


    def parse(self, response):
        item_loader = ItemLoader(ImoveisSCPropertyItem(), selector=response)

        # Header data
        item_loader.add_xpath('title', '//*[@class="visualizar-title"]/text()') #title = response.xpath('//*[@class="visualizar-title"]/text()').getall()
        # item_loader.add_xpath('code', '//*[@class="visualizar-info-codigo"]/text()')
        item_loader.add_xpath('code', '//*[@class="visualizar-header-extra"]/strong/text()')

        # 'top' Section data
        item_loader.add_xpath('price', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('caracteristicas_simples', '//ol[@class="visualizar-info-opcoes"]/li') #/*[@class="valores"]

        # 'descricao' Section data
        item_loader.add_xpath('description', '//*[@class="visualizar-descricao"]/descendant::*/text()')

        # 'caracteristicas' Section data
        # for li in response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li'):
        #     caracteristicas_detalhes = {}
        #     key = li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get()
        #     value = li.xpath('./ul/li/text()').getall()
        #     caracteristicas_detalhes[key] = value
        # item_loader.add_value('caracteristicas_detalhes', caracteristicas_detalhes)
        item_loader.add_xpath('caracteristicas_detalhes', '//*[@class="visualizacao-caracteristica-lista"]/li')

        # 'endereco' Section data
        item_loader.add_xpath('address', '//*[@class="visualizar-endereco-texto"]/text()')

        # 'anunciante' Section data
        advertiser = response.xpath('//*[@class="visualizar-anunciante-info-nome"]').attrib['title']
        item_loader.add_value('advertiser', advertiser)
        advertiser_creci = response.xpath('//*[@class="visualizar-anunciante-info-creci"]/text()').get() + response.xpath('//*[@class="visualizar-anunciante-info-creci"]/strong/text()').get()
        item_loader.add_xpath('advertiser_info', '//span[@class="visualizar-anunciante-info-creci"]')
        item_loader.add_value('advertiser_info', advertiser_creci)
        nav_headcrumbs = response.xpath('.//*[@id="breadcrumbs"]/descendant::a/text()').getall()
        item_loader.add_value('nav_headcrumbs', nav_headcrumbs)
        if len(nav_headcrumbs) > 1:
            item_loader.add_value('cidade', nav_headcrumbs[1])
        else:
            item_loader.add_value('cidade', "")
        # item_loader.add_value('cidade', nav_headcrumbs[1])
        # item_loader.add_xpath('cidade', '//*[@class="visualizar-info-location"]/text()')

        # auxiliary data
        item_loader.add_value('local', response.url)
        item_loader.add_value('business_type', response.url)
        item_loader.add_value('property_type', response.url)
        item_loader.add_value('url', response.url)
        item_loader.add_value('scraped_date', datetime.now()) #.isoformat(' ')
        item_loader.add_value('is_scraped', 0) #.isoformat(' ')

        loaded_item = item_loader.load_item()
        raw_values = {
            'title': response.xpath('//*[@class="visualizar-title"]/text()').extract(),
            'code': [response.xpath('//*[@class="visualizar-info-codigo"]/text()').extract(),
                     response.xpath('//*[@class="visualizar-header-extra"]/strong/text()').extract()],
            'price': response.xpath('//*[contains(@class, "visualizar-preco")]/descendant::*/text()').extract(),
            'caracteristicas_simples': response.xpath('//ol[@class="visualizar-info-opcoes"]/li').extract(), #/*[@class="valores"]
            'description': response.xpath('//*[@class="visualizar-descricao"]/descendant::*/text()').extract(),
            'caracteristicas_detalhes': response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li').extract(),
            'address': response.xpath('//*[@class="visualizar-endereco-texto"]/text()').extract(),
            'advertiser': advertiser,
            'advertiser_creci': advertiser_creci,
            'nav_headcrumbs': nav_headcrumbs,
            'cidade': nav_headcrumbs[1],
            'local': response.url,
            'business_type': response.url,
            'property_type': response.url,
            'url': response.url,
            'scraped_date': 'some_date', #datetime.now()
            'is_scraped': 0,
            # 'foo': response.xpath('').extract(),
        }

        return loaded_item


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(ImoveisSCPropertySpider) #, region="regiao oeste"
    process.start()
