# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Compose, TakeFirst, Join, MapCompose
import re
from w3lib.html import remove_tags

def cleanText(text):
    processed_text = []
    for subtext in text:
        subtext = subtext.replace('\n', '').replace('\t', '')
        #subtext = re.split('-| - |,|, ', subtext)
        subtext = subtext.lstrip()
        subtext = subtext.rstrip()
        processed_text.append(subtext)

    return processed_text


def cleanAndBreakText(text):
    for subtext in text:
        subtext = subtext.replace('\n', '').replace('\t', '')
        subtext = re.split('-| - |,|, ', subtext)
        subtext = [string.lstrip() for string in subtext]
        subtext = [string.rstrip() for string in subtext]
    return text


class ImoveisSCItem(scrapy.Item):
    id = scrapy.Field()
    title = scrapy.Field(input_processor=cleanText)
    code = scrapy.Field(input_processor=cleanText)
    price = scrapy.Field(input_processor=cleanText)
    caracteristicas_simples = scrapy.Field()
    description = scrapy.Field(input_processor=Compose(cleanText, Join(separator='<br>')))
    caracteristicas_detalhes = scrapy.Field()
    address = scrapy.Field(input_processor=cleanText)
    advertiser = scrapy.Field(output_processor=TakeFirst())
    advertiser_info = scrapy.Field(input_processor=MapCompose(remove_tags))
    url = scrapy.Field(output_processor=TakeFirst())
    date = scrapy.Field(output_processor=TakeFirst())

    
class ImoveisSCCatalogItem(scrapy.Item):
    title = scrapy.Field()
    code = scrapy.Field()
    local = scrapy.Field()
    description = scrapy.Field(input_processor=cleanText)
    url = scrapy.Field()
    date = scrapy.Field()


class RealestateScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
