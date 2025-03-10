from typing import Optional
from sqlalchemy import create_engine, Column, String
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column, DeclarativeBase, Session
from scrapy.utils.project import get_project_settings
import os


class Base(DeclarativeBase):
    pass

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlaclchemy engine instance.
    """
    url = get_project_settings().get("CONNECTION_STRING")
    os.makedirs('scraped_data', exist_ok=True)
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


class ImoveisSCProperty(Base):
    __tablename__ = "imoveis_sc_property"

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    code = Column(String(20))
    price = Column(String(20))
    caracteristicas_simples = Column(String(50))
    description = Column(String(1000))
    caracteristicas_detalhes = Column(String(50))
    address = Column(String(100))
    cidade = Column(String(20))
    advertiser = Column(String(50))
    advertiser_info = Column(String(20))
    nav_headcrumbs = Column(String(30))
    local = Column(String(20))
    business_type = Column(String(20))
    property_type = Column(String(20))
    url = Column(String(200))
    # is_scraped = Column(Integer)
    scraped_date = Column(DateTime)




