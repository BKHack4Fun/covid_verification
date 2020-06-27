from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

process.crawl('ncov')
process.start() 

# import shutil
# import os

# source = "data/*"
# dest = ""