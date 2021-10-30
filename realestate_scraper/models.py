from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, DateTime
from scrapy.utils.project import get_project_settings

Base = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlaclchemy engine instance.
    """
    url = get_project_settings().get("CONNECTION_STRING")
    return create_engine(url)

def create_table(engine):
    Base.metadata.create_all(engine, checkfirst=True)


class ImoveisSCCatalog(Base):
    __tablename__ = "imoveis_sc_catalog"

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    code = Column(String(20))
    local = Column(Integer)
    description = Column(String(200))
    region = Column(String(50))
    scraped_date = Column(DateTime)
    url = Column(String(200))
    url_is_scraped = Column(Integer)
    url_scraped_date = Column(DateTime)