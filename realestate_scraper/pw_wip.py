
from playwright.sync_api import sync_playwright, expect
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
import numpy as np

class FooException(Exception):
    pass


# #################################################
#                    Scraper
# #################################################

class ScraperVivaReal(object):
    def __init__(self, page, writer=None):
        self.page = page
        self.writer = writer
        self.batch_items = []
    # def scrape(): pass

    def scrape_page(self):
        """
        Parses the page and yields the scraped data.
        """
        page_items = []

        results_list = self.page.locator('//*[contains(@class, "results-list")]/div')
        for result in results_list.element_handles():
            item = self.populate_item(result)
            page_items.append(item)

        if self.writer != None:
            if self.writer.write_in_batches == False:
                self.writer.write(page_items)
            else:
                self.batch_items += page_items


    def populate_item(self, result):
        address = result.query_selector('//*[@class="property-card__address"]').inner_text()
        title = result.query_selector('//*[contains(@class, "property-card__title")]').inner_text()
        price = result.query_selector('//*[contains(@class, "property-card__values")]').inner_text()

        if result.query_selector('//*[contains(@class, "property-card__amenities")]') != None:
            amenities = result.query_selector('//*[contains(@class, "property-card__amenities")]').inner_text()
        else:
            amenities = None

        _detail_locators = result.query_selector_all('//*[contains(@class, "property-card__detail-item")]')
        detail_list = [locator.inner_text() for locator in _detail_locators]
        target_url = result.query_selector('//*[contains(@class, "property-card__carousel")]/a').get_attribute('href')

        item = {
            'address': address,
            'title': title,
            'price': price,
            'amenities': amenities,
            'details': detail_list,
            'target_url': target_url,
        }
        return item

    def _scrape_target(): pass

    def next_page(self):
        locator = self.page.locator("text=Próxima página")
        is_locator_enabled = locator.get_attribute('data-disabled') == None
        if is_locator_enabled:
            locator.click()
            time.sleep(3)
        else:
            if self.writer != None:
                if self.writer.write_in_batches == True:
                    self.writer.write(self.batch_items)

            raise FooException("No more pages to scrape")




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
    def __init__(self, file_path, write_in_batches=False, append=True):
        self.file_path = file_path
        self.write_in_batches = write_in_batches
        self.append = append

    def _write(self, data: pd.DataFrame):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        file_name = f"vivareal_{timestamp}.csv"
        data.to_csv(os.path.join(self.file_path, file_name))

    def write(self, items: dict):
        new_entries = pd.DataFrame.from_dict(data=items, orient='columns')
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
        data = self.load_data()
        if self.avoid_duplicates:
            new_entries = self.filter_duplicates(data, new_entries)
        self._write(new_entries)




# #################################################
#                     Main
# #################################################


if __name__ == "__main__":
    LOGGER = logging.getLogger(__name__)

    with sync_playwright() as p:
        navigator = p.chromium.launch(headless=False)
        page = navigator.new_page()
        # page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
        page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/apartamento_residencial/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,,,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20de%20Itaparica,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20de%20Itaparica,,,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
        
        
        file_path = '/media/user/Novo volume/Python/Secondary/realestate_scraper/data'
        file_name = 'foo.csv'

        # writer = CsvWriter(
        #     file_path=file_path,
        #     write_in_batches=False,
        # )

        writer = CsvAppender(
            reader=CsvReader(file_path=file_path, file_name=file_name),
            avoid_duplicates=True,
            # duplicate_columns=['link'],
            # mode='a'
        )

        scraper = ScraperVivaReal(page=page, writer=writer)
        time.sleep(3)

        # scraper.scrape_page()

        try:
            while True:
                scraper.scrape_page()
                scraper.next_page()
        except FooException:
            foo = 42

        foo = 42





# if __name__ == "__main__":
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
