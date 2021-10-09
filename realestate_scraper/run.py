from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
#from spiders import imoveissc_catalogo_spider

# Run that thing!

process = CrawlerProcess(get_project_settings())
process.crawl('imoveis_sc_catalog')
process.start() # the script will block here until the crawling is finished
