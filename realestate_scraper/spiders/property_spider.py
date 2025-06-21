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
import settings
from items import PropertyItem
from models import CatalogModel, HtmlCatalogModel, HtmlPropertyModel, create_table, db_connect
from sqlalchemy.orm import sessionmaker


class PropertySpider(Spider):
    name = 'imoveis_sc_properties'

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
            'realestate_scraper.pipelines.DefaultValuesPropertyPipeline': 90,
            'realestate_scraper.pipelines.SavePropertyPipeline': 100,
            'realestate_scraper.pipelines.SaveBasicInfoPipeline': 110,
            'realestate_scraper.pipelines.SaveDetailsPipeline': 120,
        }
    }

    def __init__(self, start_urls=None, region=None, min_delay=3, max_delay=7, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        if start_urls != None:
            self.start_urls = start_urls
        self.min_delay = min_delay
        self.max_delay = max_delay

    def get_urls_from_db(self):
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)

        with Session() as session:
            if self.region == None:
                rows_not_scraped = session.query(CatalogModel).filter(CatalogModel.url_is_scraped == 0).all()
            else:
                rows_not_scraped = (session.query(CatalogModel)
                                        .filter(
                                            CatalogModel.url_is_scraped == 0,
                                            CatalogModel.region == self.region
                                        ).all())

            for row in rows_not_scraped:
                time.sleep(random.randint(self.min_delay, self.max_delay))
                yield Request(url=row.url, callback=self.parse, meta={'catalogo_id': row.id})

    def start_requests(self):
        if self.start_urls != []:
            yield Request(url=self.start_urls, callback=self.parse) #, meta={'catalogo_id': row.id}
        yield from self.get_urls_from_db()


    def parse(self, response):
        item_loader = ItemLoader(PropertyItem(), selector=response)

        # Header data
        item_loader.add_xpath('title', '//*[@class="visualizar-title"]/text()') #title = response.xpath('//*[@class="visualizar-title"]/text()').getall()
        # item_loader.add_xpath('code', '//*[@class="visualizar-info-codigo"]/text()')
        item_loader.add_xpath('code', '//*[@class="visualizar-header-extra"]/strong/text()')

        # 'top' Section data
        item_loader.add_xpath('price_text', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('price_value', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('maintenance_fee', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('iptu_tax', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('price_is_undefined', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')
        item_loader.add_xpath('caracteristicas_simples', '//ol[@class="visualizar-info-opcoes"]/li') #/*[@class="valores"]

        # 'descricao' Section data
        item_loader.add_xpath('description', '//*[@class="visualizar-descricao"]/descendant::*/text()')

        # 'caracteristicas' Section data
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

        # auxiliary data
        item_loader.add_value('local', response.url)
        item_loader.add_value('business_type', response.url)
        item_loader.add_value('property_type', response.url)
        item_loader.add_value('url', response.url)
        item_loader.add_value('scraped_date', datetime.now()) #.isoformat(' ')
        item_loader.add_value('is_scraped', 0)

        loaded_item = item_loader.load_item()
        return loaded_item


class FakePropertySpider(PropertySpider):
    name = 'imoveis_sc_fake_properties'

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
            'realestate_scraper.pipelines.DefaultValuesPropertyPipeline': 90,
            'realestate_scraper.pipelines.SavePropertyPipeline': 100,
            'realestate_scraper.pipelines.SaveBasicInfoPipeline': 110,
            'realestate_scraper.pipelines.SaveDetailsPipeline': 120,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'realestate_scraper.middlewares.FakePropertyResponseMiddleware': 543,
        },
    }

    def __init__(self, start_urls=None, *args, **kwargs): #region=None, 
        super().__init__(*args, **kwargs)
        # self.region = region
        # if start_urls != None:
        #     self.start_urls = start_urls
        self.min_delay = 0
        self.max_delay = 0

    def get_urls_from_db(self):
        engine = db_connect()
        create_table(engine)
        Session = sessionmaker(bind=engine)

        with Session() as session:
            rows = session.query(HtmlPropertyModel).all()
            # if self.region == None:
                # rows_not_scraped = session.query(HtmlCatalogModel).filter(HtmlCatalogModel.url_is_scraped == 0).all()
            # else:
            #     rows_not_scraped = (session.query(HtmlCatalogModel)
            #                             .filter(
            #                                 HtmlCatalogModel.url_is_scraped == 0,
            #                                 HtmlCatalogModel.region == self.region
            #                             ).all())

        for row in rows:
            time.sleep(random.randint(self.min_delay, self.max_delay))
            yield Request(url=row.url, callback=self.parse, meta={'catalogo_id': row.id})


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    # process.crawl(PropertySpider, start_urls='https://www.imoveis-sc.com.br/florianopolis/alugar/casa/centro/casa-florianopolis-centro-1325552.html') #, region="regiao oeste"
    # process.crawl(PropertySpider) #, region="regiao oeste"
    process.crawl(FakePropertySpider) #, region="regiao oeste"
    process.start()
