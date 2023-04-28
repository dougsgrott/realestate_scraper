from typing import Optional
from sqlalchemy import create_engine, Column, String
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column, DeclarativeBase, Session
from scrapy.utils.project import get_project_settings

class Base(DeclarativeBase):
    pass

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


class VivaRealCatalog(Base):
    __tablename__ = "vivareal_catalog"

    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String(20))
    address: Mapped[str] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(String(200)) # Mapped[Optional[str]]
    details: Mapped[str] = mapped_column(String(200))
    amenities: Mapped[Optional[str]] = mapped_column(String(200))
    values: Mapped[str] = mapped_column(String(200))
    target_url: Mapped[str] = mapped_column(String(200))
    # catalog_scraped_date: Mapped[DateTime] = mapped_column(DateTime)
    catalog_scraped_date: Mapped[str] = mapped_column(String(25))
    is_target_scraped: Mapped[int] = mapped_column(Integer)


if __name__ == "__main__":
    engine = db_connect()
    VivaRealCatalog.metadata.create_all(engine)

    with Session(engine) as session:
        catalog_a = VivaRealCatalog(
            type="Apartamento",
        )
        catalog_b = VivaRealCatalog(
            type="Casa",
        )
        catalog_c = VivaRealCatalog(
            type="Loft",
        )

        session.add_all([catalog_a, catalog_b, catalog_c])
        session.commit()
