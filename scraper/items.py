# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NpmItem(scrapy.Item):
    url = scrapy.Field()
    name = scrapy.Field()
    version = scrapy.Field()
    author = scrapy.Field()
    license = scrapy.Field()
    source_code = scrapy.Field()
    tar_bytes = scrapy.Field()