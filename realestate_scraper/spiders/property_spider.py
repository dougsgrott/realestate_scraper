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


from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import Spider
from scrapy import Request
from scrapy.utils.project import get_project_settings
from w3lib.html import remove_tags
from itemloaders.processors import TakeFirst

from datetime import datetime
import sys

#from realestate_scraper.models import ImoveisSCCatalog, create_table, db_connect

sys.path.append("/home/user/PythonProj/realestate_scraper/realestate_scraper")
from items import ImoveisSCPropertyItem
from models import ImoveisSCCatalog, create_table, db_connect
from sqlalchemy.orm import sessionmaker

class ImoveisSCPropertySpider(Spider):
    name = 'imoveis_sc_properties'
    # start_urls = ["https://www.imoveis-sc.com.br/florianopolis/comprar/apartamento/ingleses/apartamento-florianopolis-ingleses-877247.html?isc_source=SD"]
    
    custom_settings = {
        'MONGODB_URI': 'mongodb://localhost:27017',
        'MONGODB_DATABASE': 'imoveis-sc',
        'MONGODB_COLLECTION': 'properties',
        'MONGODB_UNIQUE_KEY': 'url',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 1,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            # 'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
            # 'realestate_scraper.pipelines.MongoDBPipeline': 100,
        }
    }

    # def __init__(self, name=None, region=None, *args, **kwargs):
    #     super(ImoveisSCPropertySpider).__init__(*args, **kwargs)
    #     self.region = region

    def start_requests(self):
        engine = db_connect()
        create_table(engine)
        factory = sessionmaker(bind=engine)
        session = factory()
        rows_not_scraped = session.query(ImoveisSCCatalog).filter(
            ImoveisSCCatalog.url_is_scraped == 0,
            ImoveisSCCatalog.region == self.region
            )
        urls_to_be_scraped = [row.url for row in rows_not_scraped]
        ids = [row.id for row in rows_not_scraped]
        #for url in urls_to_be_scraped:
        for i in range(len(urls_to_be_scraped)):
            yield Request(url=urls_to_be_scraped[i], callback=self.parse, meta={"catalogo_id": ids[i]})


    def parse(self, response):
        item_loader = ItemLoader(ImoveisSCPropertyItem(), response=response)

        # item_loader._add_value('id', response.meta["catalogo_id"])
        
        # Header data
        item_loader.add_xpath('title', '//*[@class="visualizar-title"]/text()') #title = response.xpath('//*[@class="visualizar-title"]/text()').getall()
        
        item_loader.add_xpath('code', '//*[@class="visualizar-info-codigo"]/text()')
        item_loader.add_xpath('code', '//*[@class="visualizar-header-extra"]/strong/text()')
        
        # 'top' Section data
        item_loader.add_xpath('price', '//*[contains(@class, "visualizar-preco")]/descendant::*/text()')

        item_loader.add_xpath('caracteristicas_simples', '//ol[@class="visualizar-info-opcoes"]/li/*[@class="valores"]')
        
        # 'descricao' Section data
        item_loader.add_xpath('description', '//*[@class="visualizar-descricao"]/descendant::*/text()')

        # 'caracteristicas' Section data
        for li in response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li'):
            caracteristicas_detalhes = {}
            key = li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get()
            value = li.xpath('./ul/li/text()').getall()
            caracteristicas_detalhes[key] = value
            item_loader.add_value('caracteristicas_detalhes', caracteristicas_detalhes)

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

        item_loader.add_value('cidade', nav_headcrumbs[1])
        # item_loader.add_xpath('cidade', '//*[@class="visualizar-info-location"]/text()')

        # auxiliary data
        item_loader.add_value('local', response.url)
        
        item_loader.add_value('business_type', response.url)
        
        item_loader.add_value('property_type', response.url)
        
        item_loader.add_value('url', response.url)
        
        item_loader.add_value('scraped_date', datetime.now()) #.isoformat(' ')
        
        foo = item_loader.load_item()

        yield item_loader.load_item()


class RegiaoNorteSpider(ImoveisSCPropertySpider):
    name = 'imoveis_sc_properties_norte'
    region = "regiao norte"
    custom_settings = {
        'MONGODB_URI': 'mongodb://localhost:27017',
        'MONGODB_DATABASE': 'imoveis-sc',
        'MONGODB_COLLECTION': 'regiao-norte',
        'MONGODB_UNIQUE_KEY': 'url',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 1,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.MongoDBPipeline': 100,
            'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
        }
    }


class RegiaoOesteSpider(ImoveisSCPropertySpider):
    name = 'imoveis_sc_properties_oeste'
    region = "regiao oeste"
    custom_settings = {
        'MONGODB_URI': 'mongodb://localhost:27017',
        'MONGODB_DATABASE': 'imoveis-sc',
        'MONGODB_COLLECTION': 'regiao-oeste',
        'MONGODB_UNIQUE_KEY': 'url',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 1,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.MongoDBPipeline': 100,
            'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
        }
    }


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(RegiaoNorteSpider)
    process.crawl(RegiaoOesteSpider)
    process.crawl(ImoveisSCPropertySpider)
    process.start()

