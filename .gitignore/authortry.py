import scrapy
from spark_amplify import db

class AuthorItem(scrapy.Item):
    url=scrapy.Field()


class AuthorTry(scrapy.Spider):
    name = "author_try"
    custom_settings = {
        "COOKIES_ENABLED": 0,
        "DOWNLOAD_DELAY": .1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'
    }

    def start_requests(self):
        sql = """select  url
        from author_articles



        limit 15500,200
        ;
        """

        for urls in db.query(sql):

            for url in urls:

                    item=AuthorItem()
                    item['url']=url

                    r=scrapy.Request(url,callback=self.parse)
                    r.meta['item']=item
                    yield r


    def parse(self, response):

           author_rules=['a.author','a[rel=author]','a[href*=author]',
                    '.post-author .fn', '.byline a',
                     'a.username', '.author-name',
                    '.c-byline__item a', 'span.author', '.post-author > .fn',
                     'a[itemprop*=author]', '.author','.meta']

           time_rules=['time','.date-header','.datetime']



           with open("author_result",'a') as f:

                item = response.meta['item']




                for rule in author_rules:



                    author_name=response.css(rule+'::text').extract()
                    author_link=response.css(rule+'::attr(href)').extract()


                    if len(author_name)>0  :
                        break

                    author_name=response.css(rule).css('span::text').extract()

                    if len(author_name)>0  :
                        break



                for rule in time_rules:



                    time=response.css(rule+'::text').extract()
                    if len(time)>0:

                        break

                    time=response.css(rule).css('span::text').extract()

                    if len(time)>0:

                        break




                if len(author_name)>0:
                    author_name=[author.strip('\n\t\r ') for author in author_name]
                    author_name=list(filter(None,author_name))
                    a=[]
                    for i in author_name:
                        if i not in a:
                            a.append(i)
                    author_name=a
                    f.write("%s %s \n" % (author_name,time))

                else :
                    f.write("no item%s\n" %item['url'])
