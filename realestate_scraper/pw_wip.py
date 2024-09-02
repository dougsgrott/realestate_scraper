
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

from scrapy.selector import Selector
import sys
import os


# Configure logging to write to both a file and the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', mode='a'),  # Log file handler
        logging.StreamHandler()                        # Console handler
    ]
)

class FooException(Exception):
    pass

class CompleteException(Exception):
    pass

class LastPageException(Exception):
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
            location_filter = response.xpath('//ul[contains(@class, "location__pill-list")]//li').attrib['data-value']
            location_filter_alt = ''.join([r.get() for r in response.xpath('//ul[contains(@class, "location__pill-list")]//li//span/text()')])
        except:
            location_filter = 'Error'
            location_filter_alt = 'Error'

        try:
            have_nearby_data = response.xpath('//div[@data-type="nearby"]') != []
        except:
            have_nearby_data = 'Error'

        return {
            'n_items': n_items,
            'curr_page': curr_page,
            'breadcrumb': breadcrumb,
            'location_filter': location_filter,
            'location_filter_alt': location_filter_alt,
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
            time.sleep(3)
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


class SqliteWriter(DataWriter):
    def __init__(self):
        pass


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




# #################################################
#                     Main
# #################################################


if __name__ == "__main__":
    LOGGER = logging.getLogger(__name__)

    curr_path = os.path.dirname(os.path.realpath(__file__))
    base_path = os.path.abspath(os.path.join(curr_path, os.pardir))
    # sys.path.append(base_path)
    file_path = os.path.join(base_path, 'data') #'/media/user/Novo volume/Python/Secondary/realestate_scraper/data'
    file_name = 'foo.csv'

    with sync_playwright() as p:
        # use navigator as firefox
        # navigator = p.firefox.launch(headless=False)
        navigator = p.chromium.launch(headless=False)
        page = navigator.new_page()
        # page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
        # page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/apartamento_residencial/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,,,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20de%20Itaparica,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20de%20Itaparica,,,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
        page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/nova-itaparica/#onde=,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Nova%20Itaparica,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3ENova%20Itaparica")

        # writer = CsvWriter(
        #     file_path=file_path,
        #     write_in_batches=False,
        # )

        # writer = CsvAppender(
        #     reader=CsvReader(file_path=file_path, file_name=file_name),
        #     avoid_duplicates=True,
        #     duplicate_columns=['address', 'title', 'price'],
        #     # mode='a'
        # ) 
        writer = CsvAppender(
            reader=CsvReader(file_path=file_path, file_name=file_name),
            avoid_duplicates=True,
            duplicate_columns=['address', 'title', 'values'],
            write_in_batches=True,
            # mode='a'
        )

        scraper = ScraperVivaReal(page=page, writer=writer)
        time.sleep(3)

        scraper.run()

        # scraper.scrape_page()
        # try:
        #     while True:
        #         scraper.scrape_page()
        #         scraper.next_page()
        # except FooException:
        #     foo = 42

        foo = 42





# if __name__ == "__main__":
#     filepath_1 = '/media/user/Novo volume/Python/Secondary/realestate_scraper/test_data'
#     filepath_2 = '/media/user/Novo volume/Python/Secondary/realestate_scraper/test_data/foo'
#     filename_1 = 'vivareal_ab.csv'
#     filename_2 = 'vivareal_b.csv'

#     reader_1 = CsvReader(filepath_1, filename_1)
#     reader_2 = CsvReader(filepath_1, filename_2)

#     # data_1 = reader_1.load_data()
#     data_2 = reader_2.load_data()

#     appender = CsvWriter(filepath_2, reader_1, avoid_duplicates=False) #, duplicate_columns=['address', 'title'])
#     appender.write(data_2)

#     print("EOL")






# if __name__ == "__main__":
#     """
#     vivareal_ab is initially equal to vivareal_a. As this main runs, vivareal_b is added to vivareal_ab.
#     If avoid_duplicates=True, no duplicates will be appended to vivareal_ab.
#     If avoid_duplicates=False, duplicates will be appended to vivareal_ab.
#     """
#     filepath_1 = '/media/user/Novo volume/Python/Secondary/realestate_scraper/test_data'
#     filename_1 = 'vivareal_ab.csv'
#     filename_2 = 'vivareal_b.csv'

#     reader_1 = CsvReader(filepath_1, filename_1)
#     reader_2 = CsvReader(filepath_1, filename_2)

#     # data_1 = reader_1.load_data()
#     data_2 = reader_2.load_data()

#     appender = CsvAppender(reader_1, avoid_duplicates=True) #, duplicate_columns=['address', 'title'])
#     appender.write(data_2)

#     print("EOL")





# if __name__ == "__main__":
#     filepath_1 = '/media/user/Novo volume/Python/Secondary/realestate_scraper/test_data'
#     filename_1 = 'merge_a1.csv'
#     filename_2 = 'merge_a2.csv'

#     data_a1 = pd.read_csv(os.path.join(filepath_1, filename_1), index_col=0)
#     data_a2 = pd.read_csv(os.path.join(filepath_1, filename_2), index_col=0)

#     # merged = pd.merge(data_a2, data_a1, how='inner', left_on=['address', 'title'], right_on=['address', 'title'])

#     # # Merge the DataFrames
#     # df_merged = pd.merge(data_a1, data_a2, how='inner', left_index=True, right_index=True, suffixes=('', '_remove'))    
#     # # remove the duplicate columns
#     # df_merged.drop([i for i in df_merged.columns if 'remove' in i], axis=1, inplace=True)

#     columns = ['address', 'title']
#     bool_mask = np.all([~data_a2[col].isin(data_a1[col]) for col in columns], axis=0)
#     new_entries = data_a2[bool_mask]

#     data_a3 = pd.concat([data_a1, new_entries], axis=0)

#     print("EOL")
