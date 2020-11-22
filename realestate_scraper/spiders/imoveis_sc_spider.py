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
from w3lib.html import remove_tags

from datetime import datetime
import sys

sys.path.append("/home/user/PythonProj/Scraping/realestate_scraper/realestate_scraper")
from items import ImoveisSCItem

class ImoveisSCSpider(Spider):
    name = 'imoveis_sc'
    start_urls = ['https://www.imoveis-sc.com.br/sao-francisco-do-sul/comprar/casa?page=1']  # LEVEL 1
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
    }

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.xpath('//a[contains(@class, "btn-visualizar")]/@href').extract():
            yield response.follow(follow_url, self.populate_item)
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ItemLoader(ImoveisSCItem(), response=response)        
        #item_loader.default_input_processor = MapCompose(remove_tags)

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
            #var1_name_list.append(li.xpath('.//i').attrib['class'].split('-')[-1])
            #var1_value_list.append(li.xpath('./strong/text()').get())
        
        item_loader.add_value('caracteristicas_simples', caracteristicas_simples)
        #item_loader.add_value('var1_name_list', var1_name_list)
        #item_loader.add_value('var1_value_list', var1_value_list)


        # 'descricao' Section data
        #description_header = response.xpath('//*[@class="visualizar-descricao"]/descendant::*/text()').getall()[0]
        #description = response.xpath('//*[@class="visualizar-descricao"]/descendant::*/text()').getall()[1:]
        item_loader.add_xpath('description', '//*[@class="visualizar-descricao"]/descendant::*/text()')
        item_loader.add_xpath('description_header', '//*[@class="visualizar-descricao"]/descendant::*/text()')

        # 'caracteristicas' Section data
        var2_subtitle_list = []
        var2_value_list = []
        caracteristicas_detalhes = {}
        for li in response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li'):
            key = li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get()
            value = li.xpath('./ul/li/text()').getall()
            caracteristicas_detalhes[key] = value
            #var2_subtitle_list.append(li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get())
            #var2_value_list.append(li.xpath('./ul/li/text()').getall())
        item_loader.add_value('caracteristicas_detalhes', caracteristicas_detalhes)

        # 'endereco' Section data
        item_loader.add_xpath('address', '//*[@class="visualizar-endereco-texto"]/text()')
        #address = response.xpath('//*[@class="visualizar-endereco-texto"]/text()').getall()

        # 'anunciante' Section data

        advertiser = response.xpath('//*[@class="visualizar-anunciante-info-nome"]').attrib['title']
        item_loader.add_value('advertiser', advertiser)
        #advertiser = response.xpath('//*[@class="visualizar-anunciante-info-nome"]/text()').getall()

        # auxiliary data
        item_loader.add_value('url', response.url)
        item_loader.add_value('date_year', datetime.now().year)
        item_loader.add_value('date_month', datetime.now().month)
        item_loader.add_value('date_day', datetime.now().day)
        #url = response.url
        #date_year = datetime.now().year
        #date_month = datetime.now().month
        #date_day = datetime.now().day

        item_loader.load_item()

        # item_loader.add_css("", "")
        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]').extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


process = CrawlerProcess()
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


