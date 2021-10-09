# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
from itemadapter import ItemAdapter

from sqlalchemy.orm import sessionmaker
from scrapy.exceptions import DropItem

import sys
# sys.path.append("/home/user/PythonProj/Scraping/realestate_scraper/realestate_scraper")

# from realestate_scraper.models import ImoveisSCCatalog, create_table, db_connect
from models import ImoveisSCCatalog, create_table, db_connect

import json
from itemadapter import ItemAdapter

from datetime import datetime
import logging

# from settings import redundancy, redundancy_streak, saved
import settings
# redundancy = 0
# redundancy_streak = 0
# saved = 0


logger = logging.getLogger(__name__)  # Gets or creates a logger
logger.setLevel(logging.INFO)  # set log level
# define file handler and set formatter
file_handler = logging.FileHandler('logfile.txt')
formatter    = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)  # add file handler to logger


class DuplicatesImoveisSCCatalogPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        """
        engine = db_connect()
        create_table(engine)
        self.factory = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.factory()
        exist_title = session.query(ImoveisSCCatalog).filter_by(title=item["title"]).first()
        if (exist_title is not None):
            # global redundancy, redundancy_streak
            settings.redundancy = settings.redundancy + 1
            settings.redundancy_streak = settings.redundancy_streak + 1
            raise DropItem("Duplicate item found: {}".format(item["title"]))
            session.close()
        else:
            return item
            session.close()


class SaveImoveisSCCatalogPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        """
        engine = db_connect()
        create_table(engine)
        self.factory = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        """
        Save real estate index in the database
        This method is called for every item pipeline component
        """
        session = self.factory()
        catalog = ImoveisSCCatalog()
        catalog.title = item["title"]
        catalog.code = item["code"]
        catalog.local = item["local"]
        catalog.description = item["description"]
        catalog.region = item["region"]
        catalog.scraped_date = item["scraped_date"]
        catalog.url = item["url"]
        catalog.url_scraped = False

        try:
            print('Entry added')
            session.add(catalog)
            session.commit()
            # global saved, redundancy_streak
            settings.saved = settings.saved + 1
            settings.redundancy_streak = 0
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()
        
        return item


class LoggerImoveisSCCatalogPipeline:
    def close_spider(self, spider):
        # logger = logging.getLogger(__name__)  # Gets or creates a logger
        # logger.setLevel(logging.INFO)  # set log level
        
        # # define file handler and set formatter
        # file_handler = logging.FileHandler('logfile.log')
        # formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        # file_handler.setFormatter(formatter)

        # logger.addHandler(file_handler)  # add file handler to logger

        # print("Logger...")
        logger = logging.getLogger(__name__)  # Gets or creates a logger
        logger.info("{} new items were added to the database.".format(settings.saved))
        logger.info("{} redundant items were ignored.".format(settings.redundancy))
        logger.info("Starting url - {}.".format(spider.start_urls))
        # logger.info("Scraping started at - {}".format(spider.starting_time))
        # logger.info("Scraping ended at - {}".format(spider.finishing_time))
        # logger.info("Elapsed scraping time - {} .".format(spider.elapsed_time))
        
        # print("Opa!")
        # print(spider.crawler.stats.get_stats())
        # spider.logger.info("teste - from pipeline")
        # print("{} new items were added to the database.".format(saved))
        # print("{} redundant items were ignored.".format(redundancy))
        

class UpdateCatalogDatabasePipeline(object):
    def __init__(self):
        engine = db_connect()
        create_table(engine)
        self.factory = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.factory()
        catalog = ImoveisSCCatalog()
        catalog.title = item["title"]

        scraped_row = session.query(ImoveisSCCatalog).filter_by(url=item["url"]).first()
        scraped_row.data_scraped = True

        session.commit()
        session.close()


class JsonWriterPipeline:

    def open_spider(self, spider):
        self.file = open('items.jl', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        print("Data added to JSON")
        self.file.write(line)
        return item


class RealestateScraperPipeline:
    def process_item(self, item, spider):
        return item
