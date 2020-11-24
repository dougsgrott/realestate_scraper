# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from sqlalchemy.orm import sessionmaker
from scrapy.exceptions import DropItem
from realestate_scraper.models import ImoveisSCCatalog, create_table, db_connect

class SaveImoveisSCCatalogPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        """
        Save real estate index in the database
        This method is called for every item pipeline component
        """
        session = self.Session()
        catalog = ImoveisSCCatalog()
        catalog.title = item["title"]
        catalog.code = item["code"]
        catalog.local = item["local"]
        catalog.description = item["description"]
        catalog.url = item["url"]
        catalog.date_scraped = item["date_scraped"]

        try:
            session.add(catalog)
            session.commit()

        except:
            session.rollback()
            raise

        finally:
            session.close()

        return item

class RealestateScraperPipeline:
    def process_item(self, item, spider):
        return item
