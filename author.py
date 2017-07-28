# -*- coding: utf-8 -*-
import scrapy
import MySQLdb
import re
import logging

from spark_amplify.items import AuthorItem
from spark_amplify import constants, db
from spark_amplify import author_parser as ap

logging.basicConfig(level=logging.DEBUG)


class AuthorSpider(scrapy.Spider):
    name = "author"
    custom_settings = {
        "COOKIES_ENABLED": 0,
        "DOWNLOAD_DELAY": .1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'
    }

    def _create_db_connection(self):
        return MySQLdb.connect(host='spark.cippsw3zz0rz.ap-northeast-1.rds.amazonaws.com', port=3306, user='sparkstarter', db='spark', passwd='spark007')

    def _get_article_urls(self):
        sql = ('select distinct url, url_id '
               'from google_data '
               'where substring_index(substring_index(url, \'/\', 3), \'/\', -1) in ( '
               'select domain from remove_list_v2 '
               'where type = 1 '
               ') '
               ';')
        with self._create_db_connection() as cursor:
            cursor.execute(sql)
            for row in cursor:
                yield tuple(row)

    def start_requests(self):
        # for url, url_id in self._get_article_urls():
        for url, url_id in db.query('select url, url_id from author_articles where name=\'\' and confidence in (0, 1));'):
            item = AuthorItem()
            item['url'] = url
            item['url_id'] = url_id
            r = scrapy.Request(url, callback=self.parse)
            r.meta['item'] = item
            yield r

    def _save(self, item):
        sql = 'insert into author (url_id, url, links) values (%s, %s, %s)'
        db = self._create_db_connection()
        cursor = db.cursor()
        cursor.execute(sql, (item['url_id'], item['url'], ':::'.join(item['author_links'])))
        db.commit()
        cursor.close()
        db.close()

    def parse(self, response):
        item = response.meta['item']
        # for rule in constants.RULES:
        #     result = response.css(rule).extract()
        #     if result:
        #         item['author_links'] = result
        #         break
        # if not result:
        #     links = re.findall('<a.+?>[A-Z][a-z]+?\s[A-Z][a-z]+?<.+?</a>', response.body)
        #     item['author_links'] = links
        res = ap.extract_info(str(response.body))
        if res:
            name, link, confidence = res
            db.query('update author_articles set name=%s, link=%s, confidence=%s where url_id=%s;',
                     (name, link, confidence, item['url_id']))
            logging.debug((name, link, confidence, item['url_id']))
            # self._save(item)
            yield None
