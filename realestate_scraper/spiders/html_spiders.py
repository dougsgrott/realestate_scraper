from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scrapy.exceptions import CloseSpider
from scrapy.loader import ItemLoader
from scrapy.spiders import Spider, signals
from scrapy import Request

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
from items import HtmlCatalogItem, HtmlPropertyItem
import settings


class HtmlCatalogSpider(Spider):
    name = 'imoveis_sc_html_catalog'
    handle_httpstatus_list = [404]
    redundancy_threshold = 30

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.SaveHtmlCatalogPipeline': 200,
        },
    }

    def __init__(self, start_urls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls
        self.page_items = []

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        self.page_items = []
        time.sleep(random.randint(3,7))

        yield self.populate_item(response, response.url)
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 1
    def populate_item(self, response, url):
        catalog_loader = ItemLoader(item=HtmlCatalogItem(), selector=response)
        catalog_loader.default_output_processor = TakeFirst()
        next_page_url = response.xpath('//a[@class="next"]/@href').get()

        catalog_loader.add_value('current_url', url)
        catalog_loader.add_value('scraped_date', datetime.now()) #.isoformat(' ')
        catalog_loader.add_value('next_url', next_page_url)
        catalog_loader.add_value('raw_html', response.body)
        loaded_item = catalog_loader.load_item()
        self.page_items.append(loaded_item)
        return loaded_item

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


class HtmlPropertySpider(Spider):
    name = 'imoveis_sc_html_properties'

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'realestate_scraper.pipelines.SaveHtmlPropertyPipeline': 120,
        }
    }

    def __init__(self, start_urls=None, region=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        if start_urls != None:
            self.start_urls = start_urls

    def parse(self, response):
        ads_page_links = response.xpath('.//a[contains(@class, "btn-visualizar")]/@href').extract()
        yield from response.follow_all(ads_page_links, self.populate_item)
        yield self.paginate(response)

    def populate_item(self, response):
        item_loader = ItemLoader(HtmlPropertyItem(), selector=response)
        item_loader.default_output_processor = TakeFirst()

        item_loader.add_value('url', response.url)
        item_loader.add_value('scraped_date', datetime.now())
        item_loader.add_value('raw_html', response.text)
        loaded_item = item_loader.load_item()

        return loaded_item

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(HtmlCatalogSpider, start_urls=['https://www.imoveis-sc.com.br/regiao-serra/'])
    # process.crawl(HtmlPropertySpider, start_urls=['https://www.imoveis-sc.com.br/regiao-serra/'])
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
