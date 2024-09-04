
import random
from playwright.sync_api import sync_playwright, expect
from itemloaders.processors import Compose, TakeFirst, Join, MapCompose
import re
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
import numpy as np
from fake_useragent import UserAgent
from typing import List, Optional
from sqlalchemy import create_engine, Column, String
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column, DeclarativeBase, Session
from scrapy.utils.project import get_project_settings


from scrapy.selector import Selector
import sys
import os


# Configure logging to write to both a file and the console
logging.basicConfig(
    # level=logging.INFO,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pw_scraper_log.log', mode='a'),  # Log file handler
        logging.StreamHandler()                        # Console handler
    ]
)

class FooException(Exception):
    pass

class CompleteException(Exception):
    pass

class LastPageException(Exception):
    pass

class DuplicateItem(Exception):
    pass


# #################################################
#                    Scraper
# #################################################

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst
from datetime import datetime



# ####################################################

def parse_title(input_list):
    input_string = ''.join(input_list)
    return input_string.strip()


def parse_address(input_list):
    input_string = ''.join(input_list)
    return input_string.strip()


def get_details_text(selector_list):
    text_list = []
    for html_string in selector_list:
        soup = BeautifulSoup(html_string, 'html.parser')
        text = soup.get_text()
        text_list.append(text)
    concat_text = '<br>'.join(text_list)
    return concat_text


def parse_details_text(input_string):
    # 1. Remove all newline characters
    parsed_string = input_string.replace('\n', '')

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # 4. Remove any extra spaces around the units and words
    parsed_string = re.sub(r'\s+', ' ', parsed_string)

    # Remove extra spaces around commas
    parsed_string = parsed_string.replace(' ,', ',')
    parsed_string = parsed_string.replace(', ', ',')
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')
    return parsed_string


def get_amenities_text(html_string):
    if html_string == []:
        return 'none'
    try:
        soup = BeautifulSoup(html_string[0], 'html.parser')
        text = soup.get_text()
    except:
        text = 'error'
    return text


def parse_amenities_text(input_string):
    # if '<br>' not in input_string:
    #     # replace \n\n with <br>

    parsed_string = re.sub(r'         ', '<br>', input_string)
    parsed_string = re.sub(r'\n\n+', '<br>', parsed_string)

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # remove spaces whitespace without using strip
    parsed_string = re.sub(r'\s+', ' ', parsed_string)
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')

    # remove trainling and leading <br>
    parsed_string = parsed_string.strip('<br>')

    # replace contigous <br> with single <br>
    parsed_string = re.sub(r'(<br>)+', '<br>', parsed_string)

    return parsed_string


def get_values_text(html_string):
    if html_string == []:
        return 'none'
    try:
        soup = BeautifulSoup(html_string[0], 'html.parser')
        text = soup.get_text()
    except:
        text = 'error'
    return text


def parse_values_text(input_string):
    if '<br>' not in input_string:
        # replace \n\n with <br>
        parsed_string = re.sub(r'\n\n+', '<br>', input_string)

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # remove spaces whitespace without using strip
    parsed_string = re.sub(r'\s+', ' ', parsed_string)
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')

    # remove trainling and leading <br>
    parsed_string = parsed_string.strip('<br>')

    return parsed_string


def parse_target_url(url_list):
    url = url_list[0]
    if url.startswith('/'):
        url = 'https://www.vivareal.com.br' + url
    return url


class VivaRealCatalogItem(scrapy.Item):
    type = scrapy.Field()
    address = scrapy.Field(input_processor=parse_address)
    title = scrapy.Field(input_processor=parse_title)
    details = scrapy.Field(input_processor=Compose(get_details_text, parse_details_text))
    amenities = scrapy.Field(input_processor=Compose(get_amenities_text, parse_amenities_text))
    values = scrapy.Field(input_processor=Compose(get_values_text, parse_values_text))
    target_url = scrapy.Field(input_processor=parse_target_url)
    catalog_scraped_date = scrapy.Field()
    is_target_scraped = scrapy.Field()


# Function to get a random User-Agent
def get_random_user_agent():
    ua = UserAgent()
    return ua.random


# Function to get a random proxy
def get_random_proxy():
    proxies = [
        "http://proxy1.example.com:8080",
        "http://proxy2.example.com:8080",
        # Add more proxies here
    ]
    return random.choice(proxies)


class ScraperVivaReal(object):
    def __init__(self, page, writer=None):
        self.page = page
        self.writer = writer
        self.batch_items = []
        self.n_pagination = 0
        self.n_duplicates = 0
        self.start_datetime = datetime.now()
        self.finish_datetime = None

    def scrape_page(self):
        """
        Parses the page and returns the scraped data.
        """
        print("Scraping page")
        html = self.page.content()
        sel_page = Selector(text=html)
        aux_data = self.scrape_auxiliary_data(sel_page)
        logging.debug(f"Auxiliary data: {aux_data}")
        sel_items = self.find_selectors(sel_page, aux_data) #, has_multiple_page, last_page
        page_items = [self.populate_item(sel, self.page.url) for sel in sel_items]
        self.batch_items += page_items
        # Increment the pagination counter
        self.n_pagination += 1
        return page_items

    def scrape_auxiliary_data(self, response):
        print("Scraping auxiliary data")
        try:
            next_page_locator = self.page.locator("text=Próxima página")
            is_last_page = next_page_locator.get_attribute('data-disabled') == None
        except:
            is_last_page = 'Error'

        try:
            n_items_text = BeautifulSoup(response.xpath('//h1[contains(@class, "results-summary__title")]/strong').get(), 'html.parser').get_text()
            n_items = int(n_items_text.split()[0]) # ALT: # n_items = int(re.search(r'\d+', n_items_text).group())
        except:
            n_items = 'Error'

        try:
            curr_page_text = response.xpath('//li[contains(@class, "pagination__item")]//button[@data-active]').attrib['data-page']
            curr_page = int(curr_page_text)
        except:
            curr_page = 'Error'

        try:
            breadcrumb_pt1 = [r.get() for r in response.xpath('//a[contains(@class, "breadcrumb__item")]/text()')]
            breadcrumb_pt2 = response.xpath('//span[contains(@class, "breadcrumb__item")]/text()')[0].get()
            breadcrumb = '> '.join(breadcrumb_pt1 + [breadcrumb_pt2])
        except:
            breadcrumb = 'Error'

        try:
            # location_filter = response.xpath('//ul[contains(@class, "location__pill-list")]//li').attrib['data-value']
            location_filter_list = [s.attrib['data-value'] for s in response.xpath('//ul[contains(@class, "location__pill-list")]//li')]
            location_filter = '<br>'.join(location_filter_list)
            # location_filter_alt = ''.join([r.get() for r in response.xpath('//ul[contains(@class, "location__pill-list")]//li//span/text()')])
        except:
            location_filter = 'Error'
            # location_filter_alt = 'Error'

        try:
            have_nearby_data = response.xpath('//div[@data-type="nearby"]') != []
        except:
            have_nearby_data = 'Error'

        return {
            'n_items': n_items,
            'curr_page': curr_page,
            'breadcrumb': breadcrumb,
            'location_filter': location_filter,
            # 'location_filter_alt': location_filter_alt,
            'is_last_page': is_last_page,
            'have_nearby_data': have_nearby_data,
        }

    def find_selectors(self, response, aux_data):
        print("Finding selectors")
        selectors = response.xpath('//*[contains(@class, "results-list")]/div')
        if not selectors:
            raise FooException("No selectors found in the response.")

        # nearby_selector = response.xpath('//div[@data-type="nearby"]')
        if aux_data['have_nearby_data']:# i = 0# while i < len(selectors) and selectors[i].attrib.get('data-type') != 'nearby':#     i += 1
            return selectors[:aux_data['n_items']]#, False, is_last_page
        return selectors#, True, is_last_page

    def write(self, items):
        if self.writer != None:
            if self.writer.write_in_batches == False:
                print("Writing items")
                self.writer.write(items)

    def write_batch(self):
        if self.writer != None:
            if self.writer.write_in_batches == True:
                print("Writing batch")
                self.writer.write(self.batch_items)

    def populate_item(self, selector, url):
        catalog_loader = ItemLoader(item=VivaRealCatalogItem(), selector=selector)
        catalog_loader.default_output_processor = TakeFirst()

        catalog_loader.add_value('type', "catalog")
        catalog_loader.add_xpath('address', './/*[@class="property-card__address"]/text()')
        catalog_loader.add_xpath('title', './/*[contains(@class, "property-card__title")]/text()')
        catalog_loader.add_xpath('details', './/*[contains(@class, "property-card__detail-item")]')
        catalog_loader.add_xpath('amenities', './/*[contains(@class, "property-card__amenities")]')
        catalog_loader.add_xpath('values', './/*[contains(@class, "property-card__values")]')
        catalog_loader.add_xpath('target_url', './/*[contains(@class, "property-card__carousel")]/a/@href')
        catalog_loader.add_value('catalog_scraped_date', datetime.now().isoformat())
        catalog_loader.add_value('is_target_scraped', 0)
        loaded_item = catalog_loader.load_item()
        return loaded_item

    def next_page(self):
        print("Next page")

        nearby_data_exists = self.page.locator('//div[@data-type="nearby"]').is_visible()
        if nearby_data_exists:
            raise LastPageException("Remaining data is not from the same location")

        locator = self.page.locator("text=Próxima página")
        is_locator_enabled = locator.get_attribute('data-disabled') == None
        if is_locator_enabled:
            locator.click()
            # sleep between 5 and 7 seconds
            time.sleep(4 + 3 * np.random.random())

        else:
            raise LastPageException("No more pages to scrape")

    def run(self):
        logging.info(f"Scraping started at {self.start_datetime}")
        try:
            while True:
                scraped_items = self.scrape_page()
                self.next_page()
                self.write(scraped_items)
        except LastPageException:
            print("Last page reached (exception)")
            self.write_batch()
            self.take_action()
        self.close_spider()
        # except CompleteException:
        #     print("Scraping complete (exception)")
        #     self.close_spider()
            # raise CompleteException("Scraping complete")

    def take_action(self):
        print("Delineating action")
        # raise CompleteException("Scraping complete")

    def close_spider(self):
        print("Closing spider")
        self.finish_datetime = datetime.now()
        # # Save aggregated data at the end using write_in_batches
        # if self.writer is not None and self.writer.write_in_batches:
        #     self.writer.write_in_batches(self.batch_items)
        
        # Log the results
        self.log_results()

    def log_results(self):
        print("Logging results")
        logging.info(f"Scraping completed.")
        logging.info(f"Total pages scraped: {self.n_pagination}")
        logging.info(f"Total items scraped: {len(self.batch_items)}")
        logging.info(f"Total duplicates found: {self.n_duplicates}")
        logging.info(f"Scraping ended at {self.finish_datetime}")


# #################################################
#                Writers and Loaders
# #################################################

class DataWriter(object):
    def __init__(self):
        pass


class DataLoader(object):
    def __init__(self):
        pass


class JsonLineWriter(DataWriter):
    def __init__(self):
        pass


class CsvReader(DataLoader):
    def __init__(self, file_path, file_name):
        self.file_path = file_path
        self.file_name = file_name

    def load_data(self):
        data = pd.DataFrame()
        if os.path.exists(os.path.join(self.file_path, self.file_name)):
            data = pd.read_csv(os.path.join(self.file_path, self.file_name), index_col=0)
            
        return data


class CsvWriter(DataWriter):
    def __init__(self, file_path, reader=None, write_in_batches=False, avoid_duplicates=True,  duplicate_columns=[]): #append=True, 
        self.reader = reader
        self.file_path = file_path
        self.write_in_batches = write_in_batches
        self.avoid_duplicates = avoid_duplicates
        self.duplicate_columns = duplicate_columns
        # self.append = append

    def load_data(self):
        data = self.reader.load_data()
        return data

    def _write(self, data: pd.DataFrame):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        file_name = f"vivareal_{timestamp}.csv"
        data.to_csv(os.path.join(self.file_path, file_name))

    def filter_duplicates(self, data, new_entries):
        if data.empty:
            return new_entries
        columns = self.duplicate_columns
        if self.duplicate_columns == []:
            columns = data.columns
        bool_mask = np.any([~new_entries[col].isin(data[col]) for col in columns], axis=0)
        new_entries = new_entries[bool_mask]
        return new_entries

    def write(self, items: dict):
        new_entries = pd.DataFrame.from_dict(data=items, orient='columns')
        if new_entries.empty:
            return
        if self.reader is not None:
            data = self.load_data()
            if self.avoid_duplicates:
                new_entries = self.filter_duplicates(data, new_entries)
        self._write(new_entries)


class Base(DeclarativeBase):
    pass

class SqliteWriter(DataWriter):

    def __init__(self,
        # db_path: str,# table_name: str,# columns: List[str],
        avoid_duplicates: bool,
        duplicate_columns: List[str] = [], #None
    ):
        # self.db_path = db_path# self.table_name = table_name# self.columns = columns
        self.avoid_duplicates = avoid_duplicates
        self.duplicate_columns = duplicate_columns or []

        # self.conn = sqlite3.connect(self.db_path)# self.cursor = self.conn.cursor()# self.create_table()
        # ###############
        engine = self.db_connect()
        self.create_table(engine)
        self.factory = sessionmaker(bind=engine)

    def db_connect(self):
        """
        Performs database connection using database settings from settings.py.
        Returns sqlaclchemy engine instance.
        """
        url = get_project_settings().get("CONNECTION_STRING")
        return create_engine(url)

    def create_table(self, engine):
        Base.metadata.create_all(engine, checkfirst=True)

    def check_duplicate(self, item): #, session=None
        session = self.factory()
        # session = session if session is not None else self.factory()
        exist_title = session.query(VivaRealCatalog).filter_by(title=item["title"]).first()
        session.close()
        if (exist_title is not None):
            print("Duplicate item found: {}".format(item["title"]))
            # raise DropItem("Duplicate item found: {}".format(item["title"]))
        else:
            return item

    def write(self, item): #, session=None
        # session = self.factory()
        try:
            self.check_duplicate(item) #, session
            self.process_item(item) #, session
        except DuplicateItem as e:
            print(e)
        # finally:
        #     session.close()

    def process_item(self, item):
        """
        This method is called for every item pipeline component
        """
        session = self.factory()
        # session = session if session is not None else self.factory()
        catalog = VivaRealCatalog()
        catalog.type = item["type"]
        catalog.address = item["address"]
        catalog.title = item["title"]
        catalog.details = item["details"]
        catalog.amenities = item["amenities"]
        catalog.values = item["values"]
        catalog.target_url = item["target_url"]
        catalog.catalog_scraped_date = item['catalog_scraped_date']
        catalog.is_target_scraped = item["is_target_scraped"]

        try:
            print('Entry added')
            session.add(catalog)
            session.commit()
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()

        return item


class DataAppender(object):
    def __init__(self):
        pass


class CsvAppender(DataAppender):
    def __init__(self, reader, write_in_batches=False, avoid_duplicates=False, duplicate_columns=[], mode='a'):
        self.reader = reader
        self.write_in_batches = write_in_batches
        self.mode = mode
        self.avoid_duplicates = avoid_duplicates
        self.duplicate_columns = duplicate_columns

    def load_data(self):
        data = self.reader.load_data()
        return data

    def _write(self, data: pd.DataFrame):
        complete_filepath = os.path.join(self.reader.file_path, self.reader.file_name)
        header = not os.path.exists(complete_filepath)
        data.to_csv(complete_filepath, mode=self.mode, header=header)

    def filter_duplicates(self, data, new_entries):
        if data.empty:
            return new_entries
        columns = self.duplicate_columns
        if self.duplicate_columns == []:
            columns = data.columns
        bool_mask = np.any([~new_entries[col].isin(data[col]) for col in columns], axis=0)
        new_entries = new_entries[bool_mask]
        return new_entries

    def write(self, items: list):
        new_entries = pd.DataFrame.from_dict(data=items, orient='columns')
        if new_entries.empty:
            return
        data = self.load_data()
        if self.avoid_duplicates:
            new_entries = self.filter_duplicates(data, new_entries)
        self._write(new_entries)


class VivaRealCatalog(Base):
    __tablename__ = "vivareal_catalog"

    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String(20))
    address: Mapped[str] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(String(200)) # Mapped[Optional[str]]
    details: Mapped[str] = mapped_column(String(200))
    amenities: Mapped[Optional[str]] = mapped_column(String(200))
    values: Mapped[str] = mapped_column(String(200))
    target_url: Mapped[str] = mapped_column(String(200))
    # catalog_scraped_date: Mapped[DateTime] = mapped_column(DateTime)
    catalog_scraped_date: Mapped[str] = mapped_column(String(30))
    is_target_scraped: Mapped[int] = mapped_column(Integer)


# address: "Rua Doutor Otorino Avancini, 606 - Nova Itaparica, Vila Velha - ES"
# amenities: Portão eletrônico Área de serviço Armário na cozinha Armário no banheiro Box blindex
# catalog_scraped_date: 2024-09-03T12:37:09.792558
# details: 55 m²<br>2 Quartos<br>1 Banheiro<br>-- Vaga
# is_target_scraped: 0
# target_url: https://www.vivareal.com.br/imovel/apartamento-2-quartos-nova-itaparica-bairros-vila-velha-55m2-aluguel-RS1300-id-2738160163/
# title: "Apartamento com 2 Quartos para Aluguel, 55m²"
# type: catalog
# values: R$ 1.300 /Mês


# #################################################
#                     Main
# #################################################


# if __name__ == "__main__":
#     LOGGER = logging.getLogger(__name__)

#     curr_path = os.path.dirname(os.path.realpath(__file__))
#     base_path = os.path.abspath(os.path.join(curr_path, os.pardir))
#     file_path = os.path.join(base_path, 'data') #'/media/user/Novo volume/Python/Secondary/realestate_scraper/data'
#     file_name = 'foo.csv'

#     with sync_playwright() as p:
#         # use browser as firefox
#         # browser = p.firefox.launch(headless=False)
#         # browser = p.chromium.launch(headless=False)
#         # context = browser.new_context()
#         # page = browser.new_page()
#                     # page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/apartamento_residencial/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,,,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20de%20Itaparica,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20de%20Itaparica,,,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")

#         urls = [
#             # Vila Velha, Nova Itaparica
#             "https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/nova-itaparica/#onde=,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Nova%20Itaparica,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3ENova%20Itaparica",
#             # Vila Velha, Nova Itaparica and Jockey de Itaparica
#             "https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/nova-itaparica/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Nova%20Itaparica,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3ENova%20Itaparica,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Jockey%20de%20Itaparica,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EJockey%20de%20Itaparica,,,",
#             # Vila Velha, Itapuã and Praia da Costa
#             "https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial",
#         ]

#         # writer = CsvWriter(#     file_path=file_path,#     write_in_batches=False,# )

#         writer = CsvAppender(
#             reader=CsvReader(file_path=file_path, file_name=file_name),
#             avoid_duplicates=True,
#             duplicate_columns=['address', 'title', 'values'],
#             write_in_batches=True,
#         )

#         # writer = SqliteWriter(
#         #     avoid_duplicates=True,
#         #     duplicate_columns=['address', 'title', 'values'],
#         # )

#         for url in urls:
#             browser = p.chromium.launch(headless=False)
#             context = browser.new_context()
#             page = browser.new_page()

#             scraper = ScraperVivaReal(page=page, writer=writer)

#             # Set random User-Agent# user_agent = get_random_user_agent()# context.set_extra_http_headers({"User-Agent": user_agent})
#             # Set random proxy# proxy = get_random_proxy()# context.set_proxy({"server": proxy})

#             page.goto(url)
#             scraper.page.wait_for_load_state('load')

#             time.sleep(random.uniform(5, 10))  # Random delay between 5 to 10 seconds
#             scraper.run()
#             browser.close()
#             time.sleep(3)

if __name__ == "__main__":

    input_str = """
    ,address,amenities,catalog_scraped_date,details,is_target_scraped,target_url,title,type,values
    0,"Rua Doutor Otorino Avancini, 606 - Nova Itaparica, Vila Velha - ES","Portão eletrônico Área de serviço Armário na cozinha Armário no banheiro Box blindex",2024-09-03T12:37:09.792558,"55 m²<br>2 Quartos<br>1 Banheiro<br>-- Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-nova-itaparica-bairros-vila-velha-55m2-aluguel-RS1300-id-2738160163/,"Apartamento com 2 Quartos para Aluguel, 55m²",catalog,"R$ 1.300 /Mês"
    1,"Rua Vinícius de Morais - Nova Itaparica, Vila Velha - ES","none",2024-09-03T12:37:09.802709,"430 m²<br>-- Quarto<br>-- Banheiro<br>-- Vaga",0,https://www.vivareal.com.br/imovel/galpao-deposito-armazem-nova-itaparica-bairros-vila-velha-430m2-aluguel-RS7500-id-2715660696/,"Galpão/Depósito/Armazém para Aluguel, 430m²",catalog,"R$ 7.500 /Mês"
    0,"Rua Coronel Otto Netto - Jockey de Itaparica, Vila Velha - ES","Piscina<br>Churrasqueira<br>Elevador<br>Varanda<br>Academia<br>...",2024-09-03T12:37:23.169182,"54 m²<br>2 Quartos<br>2 Banheiros<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-54m2-aluguel-RS2000-id-2734570841/,"Apartamento com 2 Quartos para Aluguel, 54m²",catalog,"R$ 2.000 /Mês<br>Condomínio: R$ 384"
    1,"Avenida dos Estados, 10 - Jockey de Itaparica, Vila Velha - ES","Portão eletrônico<br>Elevador<br>Bicicletário",2024-09-03T12:37:23.181741,"68 m²<br>2 Quartos<br>2 Banheiros<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-68m2-aluguel-RS1550-id-2737910909/,"Apartamento com 2 Quartos para Aluguel, 68m²",catalog,"R$ 1.550 /Mês<br>Preço abaixo do mercado<br>Condomínio: R$ 250"
    2,"Rua Porto Seguro, 286 - Jockey de Itaparica, Vila Velha - ES","Lavanderia<br>Portão eletrônico",2024-09-03T12:37:23.203799,"35 m²<br>1 Quarto<br>1 Banheiro<br>-- Vaga",0,https://www.vivareal.com.br/imovel/kitnet-1-quartos-jockey-de-itaparica-bairros-vila-velha-35m2-aluguel-RS950-id-2706987955/,"Apartamento com Quarto para Aluguel, 35m²",catalog,"R$ 950 /Mês"
    3,"Avenida Ceará, 500 - Jockey de Itaparica, Vila Velha - ES","Mobiliado<br>Churrasqueira<br>Condomínio fechado<br>Aceita animais<br>Playground<br>...",2024-09-03T12:37:23.220110,"51 m²<br>2 Quartos<br>1 Banheiro<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-51m2-aluguel-RS1700-id-2733888516/,"Apartamento com 2 Quartos para Aluguel, 51m²",catalog,"R$ 1.700 /Mês<br>Condomínio: R$ 147"
    4,"Avenida Ceará, 300 - Jockey de Itaparica, Vila Velha - ES","Condomínio fechado",2024-09-03T12:37:23.234845,"50 m²<br>2 Quartos<br>1 Banheiro<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-50m2-aluguel-RS1100-id-2737985914/,"Apartamento com 2 Quartos para Aluguel, 50m²",catalog,"R$ 1.100 /Mês<br>Condomínio: R$ 300"
    5,"Avenida Ceará - Jockey de Itaparica, Vila Velha - ES","Condomínio fechado",2024-09-03T12:37:23.242894,"52 m²<br>2 Quartos<br>1 Banheiro<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-52m2-aluguel-RS1700-id-2736845623/,"Apartamento com 2 Quartos para Aluguel, 52m²",catalog,"R$ 1.700 /Mês<br>Condomínio: R$ 290"
    6,"Rua Coronel Otto Netto - Jockey de Itaparica, Vila Velha - ES","Piscina<br>Churrasqueira<br>Elevador<br>Academia<br>Aceita animais<br>...",2024-09-03T12:37:23.263289,"60 m²<br>2 Quartos<br>2 Banheiros<br>1 Vaga",0,https://www.vivareal.com.br/imovel/apartamento-2-quartos-jockey-de-itaparica-bairros-vila-velha-com-garagem-60m2-aluguel-RS2300-id-2733818315/,"Apartamento com 2 Quartos para Aluguel, 60m²",catalog,"R$ 2.300 /Mês<br>Condomínio: R$ 350"
    """

    # Split the input string into lines
    lines = input_str.strip().split('\n')

    # Extract the header and the data rows
    header = lines[0].split(',')
    data_rows = lines[1:]

    # Parse the data rows into a list of dictionaries
    data = []
    for row in data_rows:
        # Use regex to split the row by commas, but ignore commas inside quotes
        row_data = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', row)
        # Remove quotes from each field
        row_data = [field.strip('"') for field in row_data]
        # Create a dictionary for the row
        row_dict = dict(zip(header, row_data))
        data.append(row_dict)

    # Create a pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Display the DataFrame
    print(df)


    some_item = {
        'address': 'Rua Doutor Otorino Avancini, 606 - Nova Itaparica, Vila Velha - ES',
        'amenities': 'Portão eletrônico Área de serviço Armário na cozinha Armário no banheiro Box blindex',
        'catalog_scraped_date': '2024-09-03T22:46:31.381278',
        'details': '55 m²<br>2 Quartos<br>1 Banheiro<br>-- Vaga',
        'is_target_scraped': 0,
        'target_url': 'https://www.vivareal.com.br/imovel/apartamento-2-quartos-nova-itaparica-bairros-vila-velha-55m2-aluguel-RS1300-id-2738160163/',
        'title': 'Apartamento com 2 Quartos para Aluguel, 55m²',
        'type': 'catalog',
        'values': 'R$ 1.300 /Mês'
    }

    sql_writer = SqliteWriter(avoid_duplicates=True)
    sql_writer.write(some_item)

    print("EOL")