from sqlalchemy import create_engine, Column, Table, ForeignKey, MetaData
from sqlalchemy.org import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, Date, DateTime, Float, Boolean, Text, JSON
from scrapy.utils.project import get_project_settings

Base = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlaclchemy engine instance.
    """
    foo = get_project_settings().get("CONNECTION_STRING")
    return create_engine(get_project_settings().get("CONNECTION_STRING"))


def create_table(engine):
    Base.metadata.create_all(engine)


class ImoveisSCCatalog(Base):
    __tablename__ = "imoveis_sc"

    id = Column(Integer, primary_key=True)
    title = Column('title', String(200))
    code = Column('code', String(20))
    local = Column('local', Integer)
    description = Column('description')
    url = Column('url')
    date_scraped = Column('date_scraped')
