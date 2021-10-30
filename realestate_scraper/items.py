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
        subtext = subtext.lstrip()
        subtext = subtext.rstrip()
        if subtext == '':
            continue
        processed_text.append(subtext)

    return processed_text


def cleanAndBreakText(text):
    for subtext in text:
        subtext = subtext.replace('\n', '').replace('\t', '')
        subtext = re.split('-| - |,|, ', subtext)
        subtext = [string.lstrip() for string in subtext]
        subtext = [string.rstrip() for string in subtext]
    return text

def getLocal(text):
    substring = text.split('/')[3]
    substring = substring.replace('-', ' ')
    return substring


def getBusinessType(text):
    substring = text.split('/')[4]
    substring = substring.replace('-', ' ')
    return substring


def getPropertyType(text):
    substring = text.split('/')[5]
    substring = substring.replace('-', ' ')
    return substring


def dropDuplicate(collected_data):
    return list(set(collected_data))


def getCidade(collected_data):
    cidade = collected_data[0].split(',')[-1].lstrip()
    return cidade


class ImoveisSCPropertyItem(scrapy.Item):
    # id = scrapy.Field(output_processor=TakeFirst())
    title = scrapy.Field(input_processor=cleanText, output_processor=TakeFirst())
    code = scrapy.Field(input_processor=cleanText, output_processor=TakeFirst())
    price = scrapy.Field(input_processor=cleanText)
    caracteristicas_simples = scrapy.Field(input_processor=MapCompose(remove_tags))
    description = scrapy.Field(input_processor=Compose(cleanText, Join(separator='<br>')), output_processor=TakeFirst())
    caracteristicas_detalhes = scrapy.Field()
    address = scrapy.Field(input_processor=cleanText, output_processor=TakeFirst())
    # cidade = scrapy.Field(input_processor=cleanText, output_processor=getCidade)
    cidade = scrapy.Field(output_processor=TakeFirst())
    advertiser = scrapy.Field(output_processor=TakeFirst())
    advertiser_info = scrapy.Field(input_processor=MapCompose(remove_tags), output_processor=TakeFirst())
    nav_headcrumbs = scrapy.Field()
    local = scrapy.Field(input_processor=MapCompose(getLocal), output_processor=TakeFirst())
    business_type = scrapy.Field(input_processor=MapCompose(getBusinessType), output_processor=TakeFirst())
    property_type = scrapy.Field(input_processor=MapCompose(getPropertyType), output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())
    scraped_date = scrapy.Field(output_processor=TakeFirst())


class ImoveisSCCatalogItem(scrapy.Item):
    type = scrapy.Field()
    title = scrapy.Field()
    code = scrapy.Field()
    local = scrapy.Field()
    description = scrapy.Field(input_processor=cleanText)
    region = scrapy.Field(input_processor=MapCompose(getLocal))
    scraped_date = scrapy.Field()
    url = scrapy.Field()
    url_is_scraped = scrapy.Field()
    url_scraped_date = scrapy.Field()


class ImoveisSCStatusItem(scrapy.Item):
    type = scrapy.Field()
    title = scrapy.Field()
    code = scrapy.Field()
    url = scrapy.Field()
    is_scraped = scrapy.Field()
    scraped_date = scrapy.Field()
