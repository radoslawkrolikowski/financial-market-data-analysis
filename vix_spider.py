from billiard import Process
from scrapy import Spider
from scrapy import signals as scrapy_signals
from scrapy.crawler import Crawler
from kafka import KafkaProducer
from twisted.internet import reactor
from config import user_agent
from datetime import datetime
import logging
import json

# Set logger level
logging.basicConfig(level=logging.DEBUG)


class VIXCollectorPipeline:
    """Implementation of the Scrapy Pipeline that sends scraped VIX data
    through Kafka producer.

    Parameters
    ----------
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    """
    def __init__(self, server, topic):
        self.server = server
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=server,
            value_serializer=lambda x:
            json.dumps(x).encode('utf-8'))

    def process_item(self, item, spider):
        self.item = item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(server=crawler.spider.server,
            topic=crawler.spider.topic)

    def close_spider(self, spider):
        # Send VIX data through kafka producer
        self.producer.send(topic=self.topic, value=self.item)
        self.producer.flush()
        self.producer.close()


class VIXSpiderSpider(Spider):
    """Implementation of the Scrapy Spider that extracts VIX data from cnbc.com

    Parameters
    ----------
    current_dt: datetime.datetime()
        Timestamp of real-time data (EST).
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    Yields
    ------
    dict
        Dictionary that represents scraped item.

    """
    name = 'vix_reports_spider'
    allowed_domains = ['www.cnbc.com']
    start_urls = ['https://www.cnbc.com/quotes/?symbol=.VIX']
    custom_settings = {
        'ITEM_PIPELINES': {
            'vix_spider.VIXCollectorPipeline': 100
        }
    }

    def __init__(self, current_dt, server, topic):

        super(VIXSpiderSpider, self).__init__()

        self.current_dt = datetime.strftime(current_dt, "%Y-%m-%d %H:%M:%S")
        self.server = server
        self.topic = topic

    def parse(self, response):
        vix = response.xpath("//span[@class='last original']/text()").extract_first()

        yield {'VIX': float(vix),
            'Timestamp': self.current_dt}


class CrawlerScript(Process):
    """Runs Spider multiple times within one script by utilizing billiard package
    (tackle the ReactorNotRestartable error).

    Parameters
    ----------
    current_dt: datetime.datetime()
        Timestamp of real-time data (EST).
    server: list
        List of Kafka brokers addresses.
    topic: str
        Specify Kafka topic to which the stream of data records will be published.

    """
    def __init__(self, current_dt, server, topic):

        Process.__init__(self)

        self.current_dt = current_dt
        self.server = server
        self.topic = topic

        self.crawler = Crawler(
          VIXSpiderSpider,
          settings={
            'USER_AGENT': user_agent
          }
        )

        self.crawler.signals.connect(reactor.stop, signal=scrapy_signals.spider_closed)

    def run(self):
        self.crawler.crawl(self.current_dt, self.server, self.topic)
        reactor.run()


def run_vix_spider(current_dt, server, topic):

    crawler = CrawlerScript(current_dt, server, topic)

    # the script will block here until the crawling is finished
    crawler.start()
    crawler.join()
