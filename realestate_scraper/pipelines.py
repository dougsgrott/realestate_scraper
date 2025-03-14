# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy.orm import sessionmaker, Session
from models import ImoveisSCCatalog, ImoveisSCProperty, CaracteristicasSimples, CaracteristicasDetalhes, create_table, db_connect
import json
from itemadapter import ItemAdapter
from datetime import datetime
import logging
import six
import pymongo
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference
import scrapy
from scrapy.exceptions import DropItem
from scrapy.exporters import BaseItemExporter
import settings


# logger = logging.getLogger(__name__)  # Gets or creates a logger
# logger.setLevel(logging.INFO)  # set log level
# # define file handler and set formatter
# file_handler = logging.FileHandler('logfile.txt')
# formatter    = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)  # add file handler to logger


def not_set(string):
    """Check if a string is None or ''.
    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(BaseItemExporter):
    """MongoDB pipeline."""

    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'scrapy-mongodb',
        'collection': 'items',
        'separate_collections': False,
        'replica_set': None,
        'unique_key': None,
        'buffer': None,
        'append_timestamp': False,
        'stop_on_duplicate': 0,
    }

    # Item buffer
    current_item = 0
    item_buffer = []

    # Duplicate key occurence count
    duplicate_key_count = 0

    def __init__(self, **kwargs):
        """Constructor."""
        super(MongoDBPipeline, self).__init__(**kwargs)
        self.logger = logging.getLogger('scrapy-mongodb-pipeline')

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

        # Versions prior to 0.25
        if not hasattr(spider, 'update_settings') and hasattr(spider, 'custom_settings'):
            self.settings.setdict(spider.custom_settings or {}, priority='project')

    def open_spider(self, spider):
        self.load_spider(spider)

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the database
        self.database = connection[self.config['database']]
        self.collections = {'default': self.database[self.config['collection']]}

        self.logger.info(u'Connected to MongoDB {0}, using "{1}"'.format(
            self.config['uri'],
            self.config['database']))

        # Get the duplicate on key option
        if self.config['stop_on_duplicate']:
            tmpValue = self.config['stop_on_duplicate']

            if tmpValue < 0:
                msg = (
                    u'Negative values are not allowed for'
                    u' MONGODB_STOP_ON_DUPLICATE option.'
                )

                self.logger.error(msg)
                raise SyntaxError(msg)

            self.stop_on_duplicate = self.config['stop_on_duplicate']

        else:
            self.stop_on_duplicate = 0

    def configure(self):
        """Configure the MongoDB connection."""
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            self.logger.warning(
                u'DeprecationWarning: MONGODB_HOST is deprecated')
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                self.logger.warning(
                    u'DeprecationWarning: MONGODB_PORT is deprecated')
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                self.logger.warning(
                    (
                        u'DeprecationWarning: '
                        u'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ))
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('separate_collections', 'MONGODB_SEPARATE_COLLECTIONS'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
            ('stop_on_duplicate', 'MONGODB_STOP_ON_DUPLICATE')
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            msg = (
                u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                u'and MONGODB_UNIQUE_KEY is not supported'
            )
            self.logger.error(msg)
            raise SyntaxError(msg)

    def process_item(self, item, spider):
        """Process the item and add it to MongoDB.
        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        item = dict(self._get_serialized_fields(item))

        item = dict((k, v) for k, v in six.iteritems(item) if v is not None and v != "")

        if self.config['buffer']:
            self.current_item += 1

            if self.config['append_timestamp']:
                item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}

            self.item_buffer.append(item)

            if self.current_item == self.config['buffer']:
                self.current_item = 0

                try:
                    return self.insert_item(self.item_buffer, spider)
                finally:
                    self.item_buffer = []

            return item

        return self.insert_item(item, spider)

    def close_spider(self, spider):
        """Be called when the spider is closed.
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        if self.item_buffer:
            self.insert_item(self.item_buffer, spider)

    def insert_item(self, item, spider):
        """Process the item and add it to MongoDB.
        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        if not isinstance(item, list):
            item = dict(item)

            if self.config['append_timestamp']:
                item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}

        collection_name, collection = self.get_collection(spider.name)

        if self.config['unique_key'] is None:
            try:
                collection.insert(item, continue_on_error=True)
                self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], collection_name))

            except errors.DuplicateKeyError:
                self.logger.debug(u'Duplicate key found')
                if (self.stop_on_duplicate > 0):
                    self.duplicate_key_count += 1
                    if (self.duplicate_key_count >= self.stop_on_duplicate):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )

        else:
            key = {}

            if isinstance(self.config['unique_key'], list):
                for k in dict(self.config['unique_key']).keys():
                    key[k] = item[k]
            else:
                key[self.config['unique_key']] = item[self.config['unique_key']]

            collection.update(key, item, upsert=True)

            self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                self.config['database'], collection_name))

        return item

    def get_collection(self, name):
        if self.config['separate_collections']:
            collection = self.collections.get(name)
            collection_name = name

            if not collection:
                collection = self.database[name]
                self.collections[name] = collection
        else:
            collection = self.collections.get('default')
            collection_name = self.config['collection']

        # Ensure unique index
        if self.config['unique_key']:
            # collection.ensure_index(self.config['unique_key'], unique=True) # Old: broke in 3.0
            collection.create_index([(self.config['unique_key'], pymongo.ASCENDING)], unique=True)

            self.logger.info(u'Ensuring index for key {0}'.format(
                self.config['unique_key']))
        return (collection_name, collection)


class MongoPipeline:

    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        return item


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
            settings.redundancy = settings.redundancy + 1
            settings.redundancy_streak = settings.redundancy_streak + 1
            raise DropItem("Duplicate item found: {}".format(item["title"]))
        else:
            return item


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
        entry = ImoveisSCCatalog()
        for k in item.keys():
            setattr(entry, k, item[k])
        try:
            print('Entry added')
            session.add(entry)
            session.commit()
            settings.saved = settings.saved + 1
            settings.redundancy_streak = 0
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()
        return item


class SaveImoveisSCPropertyPipeline(object):
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
        entry = ImoveisSCProperty()
        fields = ["title", "code", "price", "description", "address", "cidade", "advertiser", "advertiser_info", "nav_headcrumbs", "local", "business_type", "property_type", "url", "scraped_date"]
        for k in fields:
            setattr(entry, k, item[k])
        self.process_entry(entry, session)
        return item

    def process_entry(self, entry, session):
        try:
            print('Entry added')
            session.add(entry)
            session.commit()
            settings.saved = settings.saved + 1
            settings.redundancy_streak = 0
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()
        return None


class SaveCaracteristicasSimplesPipeline(object):
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
        for k, v in item['caracteristicas_simples'].items():
            entry = CaracteristicasSimples()
            entry.title = item['title']
            entry.code = item['code']
            entry.key = k
            entry.value = v
            self.process_entry(entry, session)
        return item

    def process_entry(self, entry, session):
        try:
            print('Entry added')
            session.add(entry)
            session.commit()
            settings.saved = settings.saved + 1
            settings.redundancy_streak = 0
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()
        return None


class SaveCaracteristicasDetalhesPipeline(object):
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
        for k, v in item['caracteristicas_detalhes'].items():
            entry = CaracteristicasDetalhes()
            entry.title = item['title']
            entry.code = item['code']
            entry.key = k
            entry.value = v
            self.process_entry(entry, session)
        return item

    def process_entry(self, entry, session):
        try:
            print('Entry added')
            session.add(entry)
            session.commit()
            settings.saved = settings.saved + 1
            settings.redundancy_streak = 0
        except:
            print('rollback')
            session.rollback()
            raise
        finally:
            session.close()
        return None


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
        # catalog = ImoveisSCCatalog()
        # catalog.title = item["title"]

        scraped_row = session.query(ImoveisSCCatalog).filter_by(url=item["url"]).first()
        scraped_row.url_is_scraped = 1
        scraped_row.url_scraped_date = datetime.now()

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

