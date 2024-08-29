# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Compose, TakeFirst, Join, MapCompose
import re
from w3lib.html import remove_tags
from bs4 import BeautifulSoup

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

# ####################################################

def parse_title(input_list):
    input_string = ''.join(input_list)
    return input_string.strip()


def parse_address(input_list):
    input_string = ''.join(input_list)
    return input_string.strip()


def get_details_text(selector_list):
    text_list = []
    for html_string in selector_list:
        soup = BeautifulSoup(html_string, 'html.parser')
        text = soup.get_text()
        text_list.append(text)
    concat_text = '<br>'.join(text_list)
    return concat_text


def parse_details_text(input_string):
    # 1. Remove all newline characters
    parsed_string = input_string.replace('\n', '')

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # 4. Remove any extra spaces around the units and words
    parsed_string = re.sub(r'\s+', ' ', parsed_string)

    # Remove extra spaces around commas
    parsed_string = parsed_string.replace(' ,', ',')
    parsed_string = parsed_string.replace(', ', ',')
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')
    return parsed_string


def get_amenities_text(html_string):
    if html_string == []:
        return 'none'
    try:
        soup = BeautifulSoup(html_string[0], 'html.parser')
        text = soup.get_text()
    except:
        text = 'error'
    return text


def parse_amenities_text(input_string):
    # if '<br>' not in input_string:
    #     # replace \n\n with <br>

    parsed_string = re.sub(r'         ', '<br>', input_string)
    parsed_string = re.sub(r'\n\n+', '<br>', parsed_string)

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # remove spaces whitespace without using strip
    parsed_string = re.sub(r'\s+', ' ', parsed_string)
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')

    # remove trainling and leading <br>
    parsed_string = parsed_string.strip('<br>')

    # replace contigous <br> with single <br>
    parsed_string = re.sub(r'(<br>)+', '<br>', parsed_string)

    return parsed_string


def get_values_text(html_string):
    if html_string == []:
        return 'none'
    try:
        soup = BeautifulSoup(html_string[0], 'html.parser')
        text = soup.get_text()
    except:
        text = 'error'
    return text


def parse_values_text(input_string):
    if '<br>' not in input_string:
        # replace \n\n with <br>
        parsed_string = re.sub(r'\n\n+', '<br>', input_string)

    # 2. Remove all leading and trailing spaces
    parsed_string = parsed_string.strip()

    # remove spaces whitespace without using strip
    parsed_string = re.sub(r'\s+', ' ', parsed_string)
    parsed_string = parsed_string.replace(' <br>', '<br>')
    parsed_string = parsed_string.replace('<br> ', '<br>')

    # remove trainling and leading <br>
    parsed_string = parsed_string.strip('<br>')

    return parsed_string


def parse_target_url(url_list):
    url = url_list[0]
    if url.startswith('/'):
        url = 'https://www.vivareal.com.br' + url
    return url


class VivaRealCatalogItem(scrapy.Item):
    type = scrapy.Field()
    address = scrapy.Field(input_processor=parse_address)
    title = scrapy.Field(input_processor=parse_title)
    details = scrapy.Field(input_processor=Compose(get_details_text, parse_details_text))
    amenities = scrapy.Field(input_processor=Compose(get_amenities_text, parse_amenities_text))
    values = scrapy.Field(input_processor=Compose(get_values_text, parse_values_text))
    target_url = scrapy.Field(input_processor=parse_target_url)
    catalog_scraped_date = scrapy.Field()
    is_target_scraped = scrapy.Field()


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
