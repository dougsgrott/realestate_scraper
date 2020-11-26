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
    foo = get_project_settings().get("CONNECTION_STRING")
    #foo = 'sqlite:///imoveissc_catalogo.db'
    return create_engine(get_project_settings().get("CONNECTION_STRING"))
    #return (create_engine(foo, echo=True))

def create_table(engine):
    Base.metadata.create_all(engine)


class ImoveisSCCatalog(Base):
    __tablename__ = "imoveis_sc"

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    code = Column(String(20))
    local = Column(Integer)
    description = Column(String(200))
    url = Column(String(200))
    date_scraped = Column(DateTime)

    def __init__(self, title, code, local, description, url, date_scraped):
        self.title = title
        self.code = code
        self.local = local
        self.description = description
        self.url = url
        self.date_scraped = date_scraped


#engine = create_engine('sqlite:///imoveissc_catalog.db', echo=True)
#Base.metadata.create_all(engine, checkfirst=True)

engine = db_connect()
Base.metadata.create_all(engine, checkfirst=True)
factory = sessionmaker(bind=engine)
session = factory()
#new_imovel = ImoveisSCCatalog(title)
new_imovel = ImoveisSCCatalog('titulo1', 'code1', 'jlle', 'foo', 'www', datetime.now())
session.add(new_imovel)
session.commit()

print("EOL")