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

sys.path.append("/home/user/PythonProj/Scraping/realestate_scraper/realestate_scraper")
from items import ImoveisSCItem
from models import ImoveisSCCatalog, create_table, db_connect
from sqlalchemy.orm import sessionmaker

class ImoveisSCSpider(Spider):
    name = 'imoveis_sc'
    start_urls = []
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.UpdateCatalogDatabasePipeline': 200,
            'realestate_scraper.pipelines.JsonWriterPipeline': 100,
        }
    }

    def start_requests(self):
        engine = db_connect()
        create_table(engine)
        factory = sessionmaker(bind=engine)
        session = factory()
        rows_not_scraped = session.query(ImoveisSCCatalog).filter(ImoveisSCCatalog.data_scraped==False)
        urls_to_be_scraped = [row.url for row in rows_not_scraped]
        ids = [row.id for row in rows_not_scraped]

        #for url in urls_to_be_scraped:
        for i in range(len(urls_to_be_scraped)):
            yield Request(url=urls_to_be_scraped[i], callback=self.parse, meta={"catalogo_id": ids[i]})

    def parse(self, response):
        item_loader = ItemLoader(ImoveisSCItem(), response=response)
        item_loader.default_output_processor = TakeFirst()

        item_loader._add_value('id', response.meta["catalogo_id"])

        # Header data
        item_loader.add_xpath('title', '//*[@class="visualizar-title"]/text()') #title = response.xpath('//*[@class="visualizar-title"]/text()').getall()
        item_loader.add_xpath('code', '//*[@class="visualizar-header-extra"]/strong/text()') #code = response.xpath('//*[@class="visualizar-header-extra"]/strong/text()').getall()

        # 'top' Section data
        item_loader.add_xpath('price', '//*[contains(@class, "visualizar-preco")]/strong/text()') #price = response.xpath('//*[contains(@class, "visualizar-preco")]/strong/text()').getall()
        var1_name_list = []
        var1_value_list = []
        caracteristicas_simples = {}
        for li in response.xpath('//ol[@class="visualizar-info-opcoes"]/li'):
            key = li.xpath('.//i').attrib['class'].split('-')[-1]
            value = li.xpath('./strong/text()').get()
            caracteristicas_simples[key] = value
        
        item_loader.add_value('caracteristicas_simples', caracteristicas_simples)

        # 'descricao' Section data
        item_loader.add_xpath('description', '//*[@class="visualizar-descricao"]/descendant::*/text()')
        #item_loader.add_xpath('description_header', '//*[@class="visualizar-descricao"]/descendant::*/text()')

        # 'caracteristicas' Section data
        var2_subtitle_list = []
        var2_value_list = []
        caracteristicas_detalhes = {}
        for li in response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li'):
            key = li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get()
            value = li.xpath('./ul/li/text()').getall()
            caracteristicas_detalhes[key] = value
        item_loader.add_value('caracteristicas_detalhes', caracteristicas_detalhes)

        # 'endereco' Section data
        item_loader.add_xpath('address', '//*[@class="visualizar-endereco-texto"]/text()')

        # 'anunciante' Section data
        advertiser = response.xpath('//*[@class="visualizar-anunciante-info-nome"]').attrib['title']
        item_loader.add_value('advertiser', advertiser)
        item_loader.add_xpath('advertiser_info', '//span[@class="visualizar-anunciante-info-creci"]')
        
        # auxiliary data
        item_loader.add_value('local', response.url)
        item_loader.add_value('business_type', response.url)
        item_loader.add_value('property_type', response.url)

        item_loader.add_value('url', response.url)
        item_loader.add_value('date', datetime.now().isoformat(' '))
        
        foo = item_loader.load_item()

        yield item_loader.load_item()

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl(ImoveisSCSpider)
    process.start()



# bed - quartos
# bath - suite
# car - vagas
# rule - m² (útil)
# fullrule - m² (total)

# var1 = var.replace('\n', '').replace('\t', '')
# var2 = re.split('-| - |,|, ', var1)
# var3 = [string.lstrip() for string in var2]
# var4 = [string.rstrip() for string in var3]


