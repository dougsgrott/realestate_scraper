# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from sqlalchemy.orm import sessionmaker
from scrapy.exceptions import DropItem
from realestate_scraper.models import ImoveisSCCatalog, create_table, db_connect

import json
from itemadapter import ItemAdapter

from datetime import datetime

class DuplicatesImoveisSCCatalogPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        """
        engine = db_connect()
        create_table(engine)
        self.factory = sessionmaker(bind=engine)
        #logging.info("****DuplicatesPipeline: database connected****")

    def process_item(self, item, spider):
        session = self.factory()
        exist_title = session.query(ImoveisSCCatalog).filter_by(title=item["title"]).first()
        if (exist_title is not None):
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
        catalog.url = item["url"]
        catalog.date = item["date"]
        catalog.data_scraped = False

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
        self.file.write(line)
        return item


class RealestateScraperPipeline:
    def process_item(self, item, spider):
        return item
