# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Compose, TakeFirst, Join, MapCompose
import re
from w3lib.html import remove_tags
from bs4 import BeautifulSoup
from typing import List


def strip_strings(input: List[str]):
    # Strip leading and trailing whitespace/newlines
    return [item.strip() for item in input]

def remove_empty_strings(input: List[str]):
    # Remove empty or insignificant strings
    return [item for item in input if item]

def normalize_spacing_strings(input: List[str]):
    # Normalize spacing
    return [re.sub(r'\s+', ' ', item) for item in input]

def standardize_numeric_strings(input: List[str]):
    # Standardize numeric values (Ensure consistent format for price)
    # Keeping as a string, but properly formatted
    return [item.replace('R$ ', 'R$') for item in input]


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


def get_text_beautifulsoup(selector_list):
    text_list = []
    for html_string in selector_list:
        soup = BeautifulSoup(html_string, 'html.parser')
        text = soup.get_text()
        text_list.append(text)
    return text_list


def replace_str_list(selector_list, old_str, new_str):
    return [s.replace(old_str, new_str) for s in selector_list]


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


def convert_to_str(input):
    return str(input)


def process_headcrumbs(input):
    return ' -> '.join(input)


def parse_price_selectors(input):
    cleaned_data = strip_strings(input)
    cleaned_data = normalize_spacing_strings(cleaned_data)
    cleaned_data = remove_empty_strings(cleaned_data)
    cleaned_data = '<br>'.join(cleaned_data)
    return cleaned_data


def parse_price_text(text):
    # Default values
    price_value = None
    maintenance_fee = None
    iptu_tax = None
    price_is_undefined = 0

    # Extract price
    price_match = re.search(r'R\$ ([0-9\.]*)', text)
    if price_match:
        price_value = int(price_match.group(1).replace('.', ''))
        price_is_undefined = 0
    elif 'Sob consulta' in text:
        price_value = None
        price_is_undefined = 1

    # Extract maintenance fee
    cond_match = re.search(r'COND\. R\$<br>([0-9\.]*)', text)
    if cond_match:
        maintenance_fee = int(cond_match.group(1).replace('.', ''))
    
    # Extract IPTU tax
    iptu_match = re.search(r'IPTU R\$<br>([0-9\.]*)', text)
    if iptu_match:
        iptu_tax = int(iptu_match.group(1).replace('.', ''))
    
    return {
        'price_value': price_value,
        'maintenance_fee': maintenance_fee,
        'iptu_tax': iptu_tax,
        'price_is_undefined': price_is_undefined,
    }


class CatalogItem(scrapy.Item):

    @staticmethod
    def process_type(input):
        return input

    @staticmethod
    def process_title(input):
        cleaned_data = strip_strings(input)
        cleaned_data = normalize_spacing_strings(cleaned_data)
        return cleaned_data

    @staticmethod
    def process_code(input):
        return input

    @staticmethod
    def process_local(input):
        return input

    @staticmethod
    def process_description(input):
        cleaned_data = normalize_spacing_strings(input)
        cleaned_data = '<br>'.join(cleaned_data)
        return cleaned_data

    @staticmethod
    def process_region(input):
        substring = input.split('/')[3].split('?')[0]
        substring = substring.replace('-', ' ')
        return substring

    type = scrapy.Field()
    title = scrapy.Field()
    code = scrapy.Field()
    local = scrapy.Field()
    description = scrapy.Field(input_processor=process_description)
    region = scrapy.Field(input_processor=MapCompose(process_region))
    scraped_date = scrapy.Field()
    url = scrapy.Field()
    url_is_scraped = scrapy.Field()
    url_scraped_date = scrapy.Field()


class StatusItem(scrapy.Item):
    type = scrapy.Field()
    title = scrapy.Field()
    code = scrapy.Field()
    url = scrapy.Field()
    is_scraped = scrapy.Field()
    scraped_date = scrapy.Field()


class PropertyItem(scrapy.Item):

    @staticmethod
    def process_title(input):
        cleaned_data = strip_strings(input)
        cleaned_data = normalize_spacing_strings(cleaned_data)
        return cleaned_data

    @staticmethod
    def process_code(input):
        return input

    @staticmethod
    def process_price_text(input):
        output = parse_price_selectors(input)
        return output

    @staticmethod
    def process_price_value(input):
        price_text = parse_price_selectors(input)
        price_dict = parse_price_text(price_text)
        return price_dict['price_value']

    @staticmethod
    def process_maintenance_fee(input):
        price_text = parse_price_selectors(input)
        price_dict = parse_price_text(price_text)
        return price_dict['maintenance_fee']

    @staticmethod
    def process_iptu_tax(input):
        price_text = parse_price_selectors(input)
        price_dict = parse_price_text(price_text)
        return price_dict['iptu_tax']

    @staticmethod
    def process_price_is_undefined(input):
        price_text = parse_price_selectors(input)
        price_dict = parse_price_text(price_text)
        return price_dict['price_is_undefined']

    @staticmethod
    def process_caracteristicas_simples(input):
        if len(input) == 0:
            return {None: None}

        parsed_dict = {}
        for i in input:
            soup = BeautifulSoup(i, "html.parser")
            key = soup.find("i", class_=lambda x: x and "mdi" in x)
            key = " ".join(key["class"]) if key else None
            match = re.search(r"mdi mdi-(.*?) mdi-30px", key)
            key = match.group(1) if match else None

            value = remove_tags(i)
            value = convert_to_str(value)
            value = strip_strings([value])[0]

            parsed_dict[key] = value
        return parsed_dict

    @staticmethod
    def process_description(input):
        cleaned_data = normalize_spacing_strings(input)
        cleaned_data = '<br>'.join(cleaned_data)
        return cleaned_data

    @staticmethod
    def process_caracteristicas_detalhes(input):
        if len(input) == 0:
            return {None: None}

        parsed_dict = {}
        for i in input:
            soup = BeautifulSoup(i, "html.parser")
            key = soup.find("div", class_=lambda x: x and "subtitle" in x)
            key = key.get_text() if key else None
            key = strip_strings([key])[0]

            match = re.search(r"<li>(.*?)</li>", i)
            value = match.group(1) if match else None
            value = strip_strings([value])[0]

            parsed_dict[key] = value
        return parsed_dict

    @staticmethod
    def process_address(input):
        cleaned_data = strip_strings(input)
        cleaned_data = normalize_spacing_strings(cleaned_data)
        return cleaned_data

    @staticmethod
    def process_cidade(input):
        return input

    @staticmethod
    def process_advertiser(input):
        return input

    @staticmethod
    def process_advertiser_info(input):
        return remove_tags(input[0])

    @staticmethod
    def process_nav_headcrumbs(input):
        return process_headcrumbs(input)

    @staticmethod
    def process_local(input):
        substring = input[0].split('/')[3]
        substring = substring.replace('-', ' ')
        return substring

    @staticmethod
    def process_business_type(input):
        substring = input[0].split('/')[4]
        substring = substring.replace('-', ' ')
        return substring

    @staticmethod
    def process_property_type(input):
        substring = input[0].split('/')[5]
        substring = substring.replace('-', ' ')
        return substring

    @staticmethod
    def process_url(input):
        return input

    @staticmethod
    def process_is_scraped(input):
        return input

    @staticmethod
    def process_scraped_date(input):
        return input

    title = scrapy.Field(input_processor=process_title, output_processor=TakeFirst())
    code = scrapy.Field(input_processor=process_code, output_processor=TakeFirst())
    price_text = scrapy.Field(input_processor=process_price_text, output_processor=TakeFirst())
    price_value = scrapy.Field(input_processor=process_price_value, output_processor=TakeFirst())
    maintenance_fee = scrapy.Field(input_processor=process_maintenance_fee, output_processor=TakeFirst())
    iptu_tax = scrapy.Field(input_processor=process_iptu_tax, output_processor=TakeFirst())
    price_is_undefined = scrapy.Field(input_processor=process_price_is_undefined, output_processor=TakeFirst())
    caracteristicas_simples = scrapy.Field(input_processor=process_caracteristicas_simples, output_processor=TakeFirst())
    description = scrapy.Field(input_processor=process_description, output_processor=TakeFirst())
    caracteristicas_detalhes = scrapy.Field(input_processor=process_caracteristicas_detalhes, output_processor=TakeFirst())
    address = scrapy.Field(input_processor=process_address, output_processor=TakeFirst())
    cidade = scrapy.Field(input_processor=process_cidade, output_processor=TakeFirst())
    advertiser = scrapy.Field(input_processor=process_advertiser, output_processor=TakeFirst())
    advertiser_info = scrapy.Field(input_processor=process_advertiser_info, output_processor=TakeFirst())
    nav_headcrumbs = scrapy.Field(input_processor=process_nav_headcrumbs, output_processor=TakeFirst())
    local = scrapy.Field(input_processor=process_local, output_processor=TakeFirst())
    business_type = scrapy.Field(input_processor=process_business_type, output_processor=TakeFirst())
    property_type = scrapy.Field(input_processor=process_property_type, output_processor=TakeFirst())
    url = scrapy.Field(input_processor=process_url, output_processor=TakeFirst())
    is_scraped = scrapy.Field(input_processor=process_is_scraped, output_processor=TakeFirst())
    scraped_date = scrapy.Field(input_processor=process_scraped_date, output_processor=TakeFirst())
