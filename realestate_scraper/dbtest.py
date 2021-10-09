from sqlalchemy import create_engine, Column, Table, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, Date, DateTime, Float, Boolean, Text, JSON
from scrapy.utils.project import get_project_settings

from sqlalchemy.orm import sessionmaker

from datetime import datetime
from models import ImoveisSCCatalog

Base = declarative_base()

def db_connect():
    """
    ####Performs database connection using database settings from settings.py.
    ####Returns sqlaclchemy engine instance.
    """
    url = 'sqlite:////home/user/PythonProj/Scraping/realestate_scraper/imoveis_sc_catalog.db'
    return create_engine(url)

def create_table(engine):
    Base.metadata.create_all(engine, checkfirst=True)


engine = db_connect()
create_table(engine)
factory = sessionmaker(bind=engine)
session = factory()
rows_not_scraped = session.query(ImoveisSCCatalog).filter(ImoveisSCCatalog.data_scraped==False)

urls_to_be_scraped = [row.url for row in rows_not_scraped]

for url in urls_to_be_scraped:
    print(url)

target_url = 'https://www.imoveis-sc.com.br/governador-celso-ramos/comprar/casa/sem-bairro/casa-governador-celso-ramos--735267.html'
selected_row = session.query(ImoveisSCCatalog).filter_by(url=target_url).first()


#scraped_row = session.query(ImoveisSCCatalog).filter_by(url=item["url"]).first()
print("selected_row: {}".format(selected_row))

#selected_row.data_scraped = True
dat = datetime.now()#.isoformat(' ')
selected_row.date_scraped = dat

session.commit()
session.close()
# 2020-11-28 14:24:57.491383

print("EOL")