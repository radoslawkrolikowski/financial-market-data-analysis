from billiard import Process
from scrapy import Spider, Request
from scrapy import signals as scrapy_signals
from scrapy.crawler import Crawler
from kafka import KafkaProducer
from twisted.internet import reactor
from config import user_agent
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.DEBUG)


class COTCollectorPipeline:
    def __init__(self, server, topic):
        self.server = server
        self.topic = topic
        self.items = []
        self.producer = KafkaProducer(bootstrap_servers=server,
            value_serializer=lambda x:
            json.dumps(x).encode('utf-8'))

    def process_item(self, item, spider):
        self.items.append(item)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(server=crawler.spider.server,
            topic=crawler.spider.topic)

    def close_spider(self, spider):
        # Send COT data through kafka producer
        self.producer.send(topic=self.topic, value=self.items)
        self.producer.flush()
        self.producer.close()


class COTreportsSpiderSpider(Spider):

    name = 'cot_reports_spider'
    allowed_domains = ['www.tradingster.com']
    start_urls = ['https://www.tradingster.com/cot']
    custom_settings = {
        'ITEM_PIPELINES': {
            'cot_reports_spider.COTCollectorPipeline': 100
        }
    }

    def __init__(self, report_subject, current_dt, server, topic):

        super(COTreportsSpiderSpider, self).__init__()

        self.report_subject = report_subject
        self.current_dt = datetime.strftime(current_dt, "%Y-%m-%d %H:%M:%S")
        self.server = server
        self.topic = topic

    def parse(self, response):
        tables = response.xpath(".//table")

        for table in tables:

            rows = table.xpath(".//tr")

            for row in rows:
                name = row.xpath(".//td[1]/text()").extract_first().strip()

                if self.report_subject != name:
                    continue

                report_url = row.xpath(".//td[3]/a/@href").extract_first()

                report_url = response.urljoin(report_url)

                yield Request(url=report_url, callback=self.parse_report, dont_filter=True)

    def parse_report(self, response):
        rows = response.xpath("//table/tbody/tr")

        for row in rows:
            name = row.xpath(".//strong/text()").extract_first().strip(' /')

            if not(('Asset Manager' in name) or ('Leveraged' in name) or ('Managed Money' in name)):
                continue

            long_positions = row.xpath(".//td[2]/text()").extract_first().strip()
            long_positions_change = row.xpath(".//td[2]/span/text()").extract_first()
            long_open_int = row.xpath(".//td[3]/text()").extract_first().strip(' %')

            short_positions = row.xpath(".//td[5]/text()").extract_first().strip()
            short_positions_change = row.xpath(".//td[5]/span/text()").extract_first()
            short_open_int = row.xpath(".//td[6]/text()").extract_first().strip(' %')

            yield {'Subject': self.report_subject,
                'Scrap_datetime':  self.current_dt,
                'Name': name,
                'long_positions': long_positions,
                'long_positions_change': long_positions_change,
                'long_open_int': long_open_int,
                'short_positions': short_positions,
                'short_positions_change': short_positions_change,
                'short_open_int': short_open_int}


class CrawlerScript(Process):
    def __init__(self, report_subject, current_dt, server, topic):

        Process.__init__(self)

        self.report_subject = report_subject
        self.current_dt = current_dt
        self.server = server
        self.topic = topic

        self.crawler = Crawler(
          COTreportsSpiderSpider,
          settings={
            'USER_AGENT': user_agent
          }
        )

        self.crawler.signals.connect(reactor.stop, signal=scrapy_signals.spider_closed)

    def run(self):
        self.crawler.crawl(self.report_subject, self.current_dt, self.server, self.topic)
        reactor.run()


def run_cot_spider(report_subject, current_dt, server, topic):

    crawler = CrawlerScript(report_subject, current_dt, server, topic)

    # the script will block here until the crawling is finished
    crawler.start()
    crawler.join()

