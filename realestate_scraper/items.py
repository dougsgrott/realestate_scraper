# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Compose, TakeFirst, Join
import re

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
    title = scrapy.Field(
        input_processor=cleanText,
        output_processor=TakeFirst()
    )
    code = scrapy.Field(
        input_processor=cleanText,
        output_processor=TakeFirst()
    )

    price = scrapy.Field(
        input_processor=cleanText,
        output_processor=TakeFirst()
    )

    caracteristicas_simples = scrapy.Field(
        output_processor=TakeFirst()
    )
    #var1_name_list = scrapy.Field()
    #var1_value_list = scrapy.Field()
    
    description_header = scrapy.Field(
        input_processor=cleanText,
        output_processor=TakeFirst()
    )

    description = scrapy.Field(
        input_processor=Compose(cleanText, Join(separator='<br>')),
        output_processor=TakeFirst()
    )

    #var2_subtitle_list = scrapy.Field()
    #var2_value_list = scrapy.Field()
    caracteristicas_detalhes = scrapy.Field(
        output_processor=TakeFirst()
    )

    address = scrapy.Field(
        input_processor=cleanText,
        output_processor=TakeFirst()
    )

    advertiser = scrapy.Field(output_processor=TakeFirst())

    url = scrapy.Field(output_processor=TakeFirst())
    date_year = scrapy.Field(output_processor=TakeFirst())
    date_month = scrapy.Field(output_processor=TakeFirst())
    date_day = scrapy.Field(output_processor=TakeFirst())

    


class RealestateScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
