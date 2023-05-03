
from playwright.sync_api import sync_playwright, expect
import time
import logging
from bs4 import BeautifulSoup


class ScraperVivaReal(object):
    def __init__(self, page):
        self.page = page

    # def scrape(): pass

    def scrape_page(self):
        """
        Parses the page and yields the scraped data.
        """

        results_list = self.page.locator('//*[contains(@class, "results-list")]/div')
        for result in results_list.element_handles():
            return self.populate_item(result)


    def populate_item(self, result):
        address = result.query_selector('//*[@class="property-card__address"]').inner_text()
        title = result.query_selector('//*[contains(@class, "property-card__title")]').inner_text()
        price = result.query_selector('//*[contains(@class, "property-card__values")]').inner_text()

        try:
            if result.query_selector('//*[contains(@class, "property-card__amenities")]') != None:
                amenities = result.query_selector('//*[contains(@class, "property-card__amenities")]').inner_text()
            else:
                amenities = None
        except:
            amenities = 'error'

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


# def parse_page(page):
#     """
#     Parses the page and yields the scraped data.
#     """

#     results_list = page.locator('//*[contains(@class, "results-list")]/div')
#     for result in results_list.element_handles():
#         return populate_item(result)


# def populate_item(result):
#     address = result.query_selector('//*[@class="property-card__address"]').inner_text()
#     title = result.query_selector('//*[contains(@class, "property-card__title")]').inner_text()
#     price = result.query_selector('//*[contains(@class, "property-card__values")]').inner_text()

#     try:
#         if result.query_selector('//*[contains(@class, "property-card__amenities")]') != None:
#             amenities = result.query_selector('//*[contains(@class, "property-card__amenities")]').inner_text()
#         else:
#             amenities = None
#     except:
#         amenities = 'error'

#     _detail_locators = result.query_selector_all('//*[contains(@class, "property-card__detail-item")]')
#     detail_list = [locator.inner_text() for locator in _detail_locators]
#     target_url = result.query_selector('//*[contains(@class, "property-card__carousel")]/a').get_attribute('href')

#     item = {
#         'address': address,
#         'title': title,
#         'price': price,
#         'amenities': amenities,
#         'details': detail_list,
#         'target_url': target_url,
#     }
#     return item


class FooException(Exception):
    pass


# def next_page(page):
#     locator = page.locator("text=Próxima página")
#     is_locator_enabled = locator.get_attribute('data-disabled') == None
#     if is_locator_enabled:
#         locator.click()
#         time.sleep(3)
#     else:
#         raise FooException("No more pages to scrape")


LOGGER = logging.getLogger(__name__)

with sync_playwright() as p:
    navigator = p.chromium.launch(headless=False)
    page = navigator.new_page()
    page.goto("https://www.vivareal.com.br/aluguel/espirito-santo/vila-velha/bairros/itapua/#onde=Brasil,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Itapu%C3%A3,,,,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EItapua,,,;,Esp%C3%ADrito%20Santo,Vila%20Velha,Bairros,Praia%20da%20Costa,,,neighborhood,BR%3EEspirito%20Santo%3ENULL%3EVila%20Velha%3EBarrios%3EPraia%20da%20Costa,-20.330616,-40.290992,&tipos=apartamento_residencial,flat_residencial,kitnet_residencial")
    scraper = ScraperVivaReal(page)
    time.sleep(3)

    try:
        while True:
            scraper.scrape_page()
            scraper.next_page()
    except FooException:
        foo = 42

    foo = 42


        # for locator in amenities_locators:
        #     amenities_list.append(locator.inner_text())
        # result.query_selector_all('//*[contains(@class, "property-card__detail-item")]')[1].inner_text()

    # locator = page.locator("text=Próxima página")
    # is_locator_enabled = locator.get_attribute('data-disabled') == None

    # while is_locator_enabled:
    #     locator.click()
    #     time.sleep(3)
    #     locator = page.locator("text=Próxima página")
    #     is_locator_enabled = locator.get_attribute('data-disabled') == None
    #     # print page's url
    #     _urls.append(page.url)
    #     # print(page.url)






    # # Scroll to the end of the page
    # # page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    #     # print("enabled")
    #     # LOGGER.info("enabled")




# When should the "Next" button be clicked?
#     When the CatalogueSpider asks for a new page after it scraped the content of the current page

# What could be a valid successfull condition?
#     When the "Next" button is disabled




# Ok, so... Overpass Ingestor is lost. :(
# But it is not really needed for the first part of this project