
from playwright.sync_api import sync_playwright, expect
import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
import os

class FooException(Exception):
    pass


class ScraperVivaReal(object):
    def __init__(self, page, writer=None):
        self.page = page
        self.writer = writer

    # def scrape(): pass

    def scrape_page(self):
        """
        Parses the page and yields the scraped data.
        """
        item_list = []

        results_list = self.page.locator('//*[contains(@class, "results-list")]/div')
        for result in results_list.element_handles():
            item = self.populate_item(result)
            item_list.append(item)

        if self.writer != None:
            # try:
            self.writer.write(item_list)
            # except:
            #     raise Exception("Write exception")


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
            raise FooException("No more pages to scrape")



class ItemWriter(object):
    def __init__(self):
        pass


class JsonLineWriter(ItemWriter):
    def __init__(self):
        pass


class CsvWriter(ItemWriter):
    def __init__(self, file_path, file_name, write_in_batches=False):
        self.file_path = file_path
        self.file_name = file_name
        self.write_in_batches = write_in_batches

    def load_data_foo(self):
        if os.path.exists(os.path.join(self.file_path, self.file_name)):
            data = pd.read_csv(os.path.join(self.file_path, self.file_name), index_col=0)
        else:
            data = pd.DataFrame()
        return data

    def _write(self, item: dict):
        pass
        # Load the dataframe using file_path
        # Transforms item in a dataframe
        # Concatenates both dataframes
        # Saves the concatenated dataframe

    def batch_write(self, items: list):
        data = self.load_data_foo()
        new_entries = pd.DataFrame.from_dict(data=items, orient='columns')
        concat_data = pd.concat([data, new_entries], axis=0)
        concat_data.to_csv(os.path.join(self.file_path, self.file_name))

    def write(self, items: dict):
        if self.write_in_batches:
            self.batch_write(items)
        else:
            for item in items:
                self._write(item)


class SqliteWriter(ItemWriter):
    def __init__(self):
        pass





if __name__ == "__main__":
    LOGGER = logging.getLogger(__name__)

    with sync_playwright() as p:
        navigator = p.chromium.launch(headless=False)
        page = navigator.new_page()
        page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
        
        file_path = '/media/user/Novo volume/Python/Secondary/realestate_scraper/data'
        file_name = 'foo.csv'
        writer = CsvWriter(file_path, file_name, True)

        scraper = ScraperVivaReal(page=page, writer=writer)
        time.sleep(3)

        scraper.scrape_page()

        # try:
        #     while True:
        #         scraper.scrape_page()
        #         scraper.next_page()
        # except FooException:
        #     foo = 42

        foo = 42
