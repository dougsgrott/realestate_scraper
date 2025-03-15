import logging
from scrapy.loader import ItemLoader
from items import CatalogItem, StatusItem
from sqlalchemy.orm import sessionmaker
from bs4 import BeautifulSoup
import re
from itemloaders.processors import TakeFirst
from models import db_connect, CatalogModel
from scrapy.exceptions import CloseSpider
import os


log_directory = 'logs'
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, 'imoveis_sc_catalog_planner.log')

skipper_logger = logging.getLogger("BasicSkipperLogger")
skipper_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file_path) # A separate log file for planner-related logs
formatter = logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)

# Attach the file handler to the logger
skipper_logger.addHandler(file_handler)

class BasicSkipper():

    def __init__(self, threshold, skip_n):
        self.threshold = threshold
        self.skip_n = skip_n
        self.duplicated_page_count = 0
        self.current_status = 'normal'
        self.iter = 0

        self.archive = {}

    def get_attr_to_compare(self, response, selector):
        page_items = []
        for sel in response.xpath(selector):
            catalog_loader = ItemLoader(item=CatalogItem(), selector=sel)
            catalog_loader.default_output_processor = TakeFirst()
            catalog_loader.add_xpath('title', './/h2[@class="imovel-titulo"]/a/meta[@itemprop="name"]/@content')
            catalog_loader.add_xpath('code', './/div[@class="imovel-extra"]/span/text()')
            loaded_item = catalog_loader.load_item()
            page_items.append(loaded_item)

        skipper_logger.debug("Found %d items to compare on page: %s",
                             len(page_items), response.url)
        return page_items

    def check_for_duplicates(self, item_list):
        num_duplicate = 0
        engine = db_connect()
        Session = sessionmaker(bind=engine)
        session = Session()

        for item in item_list:
            exist_entry = session.query(CatalogModel).filter_by(title=item["title"], code=item["code"]).first()
            if exist_entry:
                num_duplicate += 1

        session.close()

        if num_duplicate == len(item_list) and len(item_list) != 0:
            self.duplicated_page_count += 1
            skipper_logger.info(
                "All %d items are duplicates (page duplicates in a row: %d).",
                len(item_list), self.duplicated_page_count
            )
        else:
            self.duplicated_page_count = 0

    def get_curr_and_last_page_number(self, response):
        soup = BeautifulSoup(response.xpath('//div[@class="navigation"]').get(), 'html.parser')
        text = soup.get_text()
        match = re.search(r"de (\d+)", text)
        max_number = int(match.group(1) if match else None)
        match = re.search(r'page=(\d+)', response.url)
        curr_number = int(match.group(1)) if match else 1
        self.curr_number = curr_number
        self.max_number = max_number

        skipper_logger.debug("Current page: %d, Max page: %d", curr_number, max_number)

    def paginate_normal(self, response):
        """Return next page from pagination"""
        next_page_url = response.xpath('//a[@class="next"]/@href').get()
        skipper_logger.debug("Normal paginate -> Next page: %s", next_page_url)
        # if next_page_url is not None:
        return next_page_url

    def paginate_skipping(self, response):
        """Skip N pages"""
        self.get_curr_and_last_page_number(response)
        # print(self.curr_number+self.skip_n, self.max_number)
        new_page_number = min(self.curr_number+self.skip_n, self.max_number)
        new_url = response.url.replace(f'page={self.curr_number}', f'page={new_page_number}')

        skipper_logger.info(
            "Skipping from page %d to page %d (skip_n=%d).",
            self.curr_number, new_page_number, self.skip_n
        )
        return new_url

    def paginate_fallback(self, response):
        """Fallback N-1 pages"""
        self.get_curr_and_last_page_number(response)
        # print(self.curr_number+self.skip_n, self.max_number)
        # new_page_number = min(self.curr_number+self.skip_n, self.max_number)
        new_page_number = self.curr_number - (self.skip_n - 1)
        new_url = response.url.replace(f'page={self.curr_number}', f'page={new_page_number}')

        skipper_logger.warning(
            "Falling back from page %d to page %d after skipping logic.",
            self.curr_number, new_page_number
        )
        return new_url

    def update_status(self, ):
        dup_pages = self.duplicated_page_count >= self.threshold
        near_the_end = self.curr_number + self.skip_n >= self.max_number
        skipping_extrapolates = self.curr_number + self.skip_n >= self.max_number
        on_last_page = self.curr_number == self.max_number
        # print(dup_pages, near_the_end, skipping_extrapolates, on_last_page)

        if (self.current_status == 'normal') and on_last_page:
            return 'close'
        if (self.current_status == 'normal') and dup_pages and not near_the_end:
            return 'skipping'
        if (self.current_status == 'normal') and not dup_pages:
            return 'normal'
        if (self.current_status == 'normal') and dup_pages and skipping_extrapolates:
            return 'normal'
        if (self.current_status == 'skipping') and dup_pages:
            # Continues skipping
            return 'skipping'
        if (self.current_status == 'skipping') and not dup_pages:
            return 'fallback'
        if (self.current_status == 'skipping') and skipping_extrapolates:
            return 'normal'
        if (self.current_status == 'fallback') and not dup_pages:
            return 'normal'

        # If we get here, it’s an unexpected combination of states
        skipper_logger.error(
            "Unexpected condition while updating status. "
            "duplicated_page_count=%d, threshold=%d, current_status=%s.",
            self.duplicated_page_count, self.threshold, self.current_status
        )
        raise Exception()

    def foo(self, response, selector, parse_method):
        items_list = self.get_attr_to_compare(response, selector)
        if len(items_list) == 0:
            new_page = self.paginate_normal(response)
            skipper_logger.debug("No items found on this page -> normal pagination.")
            return new_page

        self.get_curr_and_last_page_number(response)
        self.check_for_duplicates(item_list=items_list)
        new_status = self.update_status()

        if new_status == 'normal':
            new_page = self.paginate_normal(response)
        elif new_status == 'skipping':
            new_page = self.paginate_skipping(response)
        elif new_status == 'fallback':
            new_page = self.paginate_fallback(response)
        elif new_status == 'close':
            # raise CloseSpider() #self.paginate_normal(response)
            new_page = self.paginate_normal(response)
        else:
            raise Exception()

        # print(f"Status: {self.current_status} -> {new_status}, i: {i}, page: {p} -> {new_page}")
        collection = {
            "Iteration": self.iter,
            "Current status": self.current_status,
            "Next status": new_status,
            "Current page": response.url,
            "Next page": new_page,
            "Duplicated page count": self.duplicated_page_count,
        }
        self.archive[self.iter] = collection
        self.current_status = new_status
        self.iter += 1

        skipper_logger.info(
            "Iteration %d: Transition from %s to %s. "
            "Current page: %s -> Next page: %s. Duplicates: %d",
            collection["Iteration"],
            collection["Current status"],
            collection["Next status"],
            collection["Current page"],
            collection["Next page"],
            collection["Duplicated page count"]
        )

        # print()
        # for key, value in collection.items():
        #     print(f"{key}: {value}")
        # print(collection)

        return new_page
