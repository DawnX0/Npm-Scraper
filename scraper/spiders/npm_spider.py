import scrapy
from scraper.items import NpmItem


class NpmSpider(scrapy.Spider):
    name = "npm"
    base_url = "https://www.npmjs.com/search?q=keyword%3A%20{}&page={}&perPage=100"
    registry_url = "https://registry.npmjs.com/{}"

    def __init__(self, name: str | None = None, **kwargs: any):
        super().__init__(name, **kwargs)
        self.keyword = input("Enter keyword to search for: ")
        self.max_pages = None


    async def start(self):
        yield scrapy.Request(
            url=self.base_url.format(self.keyword, 0),
            callback=self.parse,
            meta={
                "playwright": True,
                "current_page": 0
            }
        )


    async def parse(self, response: scrapy.http.Response):
        if self.max_pages is None:
            self.max_pages = int(response.xpath('//*[@id="main"]/div/div[2]/div[2]/div/nav/div[5]/a/text()').get())

        for page in range(0, self.max_pages + 1):
            print(f'[STATUS]: {page}/{self.max_pages}')
            yield scrapy.Request(
                url=self.base_url.format(self.keyword, page),
                callback=self.parse_page,
                meta={
                    "playwright": True,
                    "current_page": page
                }
            )


    async def parse_page(self, response: scrapy.http.Response):
        hrefs = [href for href in response.xpath("//a/@href").getall() if href.startswith("/package")]
        for href in hrefs:
            item = NpmItem()
            item["url"] = self.registry_url.format("/".join(href.split("/")[2:]))
            yield item


    
