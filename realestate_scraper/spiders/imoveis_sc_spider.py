#import scrapy
from scrapy.crawler import CrawlerProcess
#from datetime import datetime

#
# This file was created by Attila Toth - http://scrapingauthority.com
#
#
# This template is usable for TWO-LEVEL DEEP scrapers with pagination on the 1st level.
#
# HOW THE LOOP WORKS:
#
# 1. FOLLOWING LEVEL 1: Follow item urls.
# 2. SCRAPING LEVEL 2: Scrape the fields and populate item.
# 3. PAGINATION LEVEL 1: Go to the next page with the "next button" if any.
# 1. ...
#
#


from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import Spider
from scrapy import Request
from w3lib.html import remove_tags


class ImoveisSCSpider(Spider):
    name = 'imoveis_sc'
    start_urls = ['https://www.imoveis-sc.com.br/sao-francisco-do-sul/comprar/casa?page=1']  # LEVEL 1
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'DOWNLOAD_DELAY': 5,
        'ROBOTSTXT_OBEY': False,
    }

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.xpath('//a[contains(@class, "btn-visualizar")]/@href').extract():
            yield response.follow(follow_url, self.populate_item)
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        #item_loader = ItemLoader(item=MySpiderItem(), response=response)
        #item_loader.default_input_processor = MapCompose(remove_tags)

        # Header data
        title = response.xpath('//*[@class="visualizar-title"]/text()').get()
        code = response.xpath('//*[@class="visualizar-header-extra"]/strong/text()').get()

        # 'top' Section data
        price = response.xpath('//*[@class="visualizar-preco  has-extra "]/strong/text()').get()

        #iptu pt1 = response.xpath('//*[@class="visualizar-preco  has-extra "]/text()').getall()
        #iptu pt2 = response.xpath('//*[@class="visualizar-preco  has-extra "]/span/text()').get()
        #enredeco opt1 = response.xpath('//*[contains(@class, "icon-blue")]/../text()').getall()
        #endereco opt2 = response.xpath('//*[@class="visualizar-info-location"]/text()').getall()

        var1_name_list = []
        var1_value_list = []
        for li in response.xpath('//ol[@class="visualizar-info-opcoes"]/li'):
            var1_name_list.append(li.xpath('.//i').attrib['class'].split('-')[-1])
            var1_value_list.append(li.xpath('./strong/text()').get())

        # 'descricao' Section data
        description_header = response.xpath('//*[@class="visualizar-descricao"]/h2/text()').getall()
        description = response.xpath('//*[@class="visualizar-descricao"]/p/text()').getall()

        # 'caracteristicas' Section data
        var2_subtitle_list = []
        var2_value_list = []
        for li in response.xpath('//*[@class="visualizacao-caracteristica-lista"]/li'):
            var2_subtitle_list.append(li.xpath('.//*[@class="visualizacao-section-subtitle"]/text()').get())
            var2_value_list.append(li.xpath('./ul/li/text()').getall())

        # 'endereco' Section data
        address = response.xpath('//*[@class="visualizar-endereco-texto"]/text()').getall()

        # 'anunciante' Section data
        advertiser = response.xpath('//*[@class="visualizar-anunciante-info-nome"]').attrib['title']

        # item_loader.add_css("", "")
        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath('//a[@class="next"]').extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)


process = CrawlerProcess()
process.crawl(ImoveisSCSpider)
process.start()

# class ArticleSpider(scrapy.Spider):
#     name = "medium_basic"
#     custom_settings = {
#         'AUTOTHROTTLE_ENABLED': True,
#         'AUTOTHROTTLE_DEBUG': True,
#         'DOWNLOAD_DELAY': 5,
#         'ROBOTSTXT_OBEY': False,
#     }
#     def start_requests(self):
#         urls = [
#             'https://towardsdatascience.com/archive'
#         ]
#         for url in urls:
#             yield scrapy.Request(url=url, callback=self.parse)

#     def parse(self, response):
#         year_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[2]/*/a/@href').getall()
#         if len(year_pages) != 0:
#             yield from response.follow_all(year_pages, callback=self.parse_months)
#         else:
#             yield from self.parse_articles(response)
    
#     def parse_months(self, response):
#         month_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[3]/div/a/@href').getall()
#         if len(month_pages) != 0:
#             yield from response.follow_all(month_pages, callback=self.parse_days)
#         else:
#             yield from self.parse_articles(response)

#     def parse_days(self, response):
#         day_pages = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[4]/div/a/@href').getall()
#         if len(day_pages) != 0:
#             yield from response.follow_all(day_pages, callback=self.parse_articles)
    
#     def parse_articles(self, response):
#         articles = response.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[2]/*')
#         if len(articles) != 0:
#             for article in articles:
#                 author = article.xpath('.//a[@data-action="show-user-card"]/text()').get()
                
#                 str_read_time = article.xpath('.//*[@class="readingTime"]/@title')[0].get()
#                 int_read_time = str_read_time.split[0]

#                 collection = article.xpath('.//a[@data-action="show-collection-card"]/text()').get()
#                 title = article.xpath('.//h3[contains(@class, "title")]/text()').get()
                
#                 claps = article.xpath('.//button[@data-action="show-recommends"]/text()').get()
#                 if claps == None:
#                     claps = 0
#                 responses = article.xpath('.//a[@class="button button--chromeless u-baseColor--buttonNormal"]/text()').get()
#                 if responses == None:
#                     responses = 0
#                 subtitle_preview = article.xpath('.//h4[@name="previewSubtitle"]/text()').get()
                
#                 published_date = article.xpath('.//time/text()').get()
#                 try:
#                     date_object = datetime.strptime(published_date, "%b %d, %Y")
#                     day = date_object.day
#                     month = date_object.month
#                     year = date_object.year
#                 except:
#                     date_object = datetime.strptime(published_date, "%b %d")
#                     day = date_object.day
#                     month = date_object.month
#                     year = datetime.now().year

#                 yield {
#                     'author' : author,
#                     'title' : title,
#                     'subtitle preview' : subtitle_preview,
#                     'collection' : collection,
#                     'read time' : int_read_time,
#                     'claps' : claps,
#                     'responses' : responses,
#                     'published date' : published_date,
#                     'day' : day,
#                     'month' : month,
#                     'year' : year
#                 }






# url = "https://towardsdatascience.com/archive"
# response = requests.get(url, verify = False)
# parser = parser.fromstring(response.text)

#parser.xpath('/html/body/div[1]/div[2]/div/div[3]/div[1]/div[1]/div/div[2]/*/a/@href')




# bed - quartos
# bath - suite
# car - vagas
# rule - m² (útil)