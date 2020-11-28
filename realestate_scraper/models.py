from sqlalchemy import create_engine, Column, Table, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, Date, DateTime, Float, Boolean, Text, JSON
from scrapy.utils.project import get_project_settings

from sqlalchemy.orm import sessionmaker

from datetime import datetime

Base = declarative_base()

def db_connect():
    """
    ####Performs database connection using database settings from settings.py.
    ####Returns sqlaclchemy engine instance.
    """
    url = get_project_settings().get("CONNECTION_STRING")
    return create_engine(url)

def create_table(engine):
    Base.metadata.create_all(engine, checkfirst=True)


class ImoveisSCCatalog(Base):
    __tablename__ = "imoveis_sc"

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    code = Column(String(20))
    local = Column(Integer)
    description = Column(String(200))
    url = Column(String(200))
    date = Column(DateTime)
    data_scraped = Column(Boolean)


    # def __init__(self, title, code, local, description, url, date_scraped):
    #     self.title = title
    #     self.code = code
    #     self.local = local
    #     self.description = description
    #     self.url = url
    #     self.date_scraped = date_scraped


# engine = db_connect()
# create_table(engine)
# factory = sessionmaker(bind=engine)
# session = factory()
# #new_imovel = ImoveisSCCatalog('titulo1', 'code1', 'jlle', 'foo', 'www', datetime.now())

# new_imovel = ImoveisSCCatalog()
# new_imovel.title = 'titulo1'
# new_imovel.code = 'code1'
# new_imovel.local = 'jlle'
# new_imovel.description = 'foo'
# new_imovel.url = 'www'
# new_imovel.date_scraped = datetime.now()

# exist_title = session.query(ImoveisSCCatalog).filter_by(title=new_imovel.title).first()
# #exist_code = session.query(ImoveisSCCatalog).filter_by(code=new_imovel.code).first()

# if (exist_title is None):
#     try:
#         print('entry added')
#         session.add(new_imovel)
#         session.commit()
#     except:
#         print('rollback')
#         session.rollback()
#     finally:
#         session.close()
# else:
#     print('entry is already in db')


# print("EOL")