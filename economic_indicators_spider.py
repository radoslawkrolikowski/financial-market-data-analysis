# -*- coding: utf-8 -*-
from billiard import Process
from scrapy import Spider
from scrapy import signals as scrapy_signals
from scrapy.crawler import Crawler
from twisted.internet import reactor
from kafka import KafkaProducer
from config import user_agent, time_zone
from datetime import datetime
from collections import defaultdict
import logging
import re
import json
import pickle

logging.basicConfig(level=logging.DEBUG)


class IndicatorCollectorPipeline:
    def __init__(self, server, topic):
        self.server = server
        self.topic = topic
        self.items_dict = defaultdict()

        try:
            with open(r"items.pickle", "rb") as output_file:
                self.prev_items = pickle.load(output_file)
        except (OSError, IOError):
            with open(r"items.pickle", "wb") as output_file:
                pickle.dump({}, output_file)

        self.producer = KafkaProducer(bootstrap_servers=server,
            value_serializer=lambda x:
            json.dumps(x).encode('utf-8'))

        self.prev_items = defaultdict()

    def process_item(self, item, spider):
        self.item = item

        self.items_dict.setdefault((item['Schedule_datetime'], item['Event']), item)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(server=crawler.spider.server,
            topic=crawler.spider.topic)

    def close_spider(self, spider):
        new_items = [self.items_dict[k] for k in set(self.items_dict) - set(self.prev_items)]

        if new_items:
            # Send economic data through kafka producer
            self.producer.send(topic=self.topic, value=new_items)
            self.producer.flush()
            self.producer.close()

        with open(r"items.pickle", "wb") as output_file:
            pickle.dump(self.items_dict, output_file)


class EconomicIndicatorsSpiderSpider(Spider):

    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['https://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.IndicatorCollectorPipeline': 100
        }
    }

    def __init__(self, countries, importance,  event_list, current_dt, server, topic):

        super(EconomicIndicatorsSpiderSpider, self).__init__()

        self.countries = countries
        self.importance = ['bull' + x for x in importance]
        self.event_list = event_list
        self.current_dt = current_dt
        self.server = server
        self.topic = topic

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")

        for event in events:

            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()
            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])

            current_dt_str = datetime.strftime(self.current_dt, "%Y/%m/%d %H:%M:%S")

            # Return only events that passed
            if not self.current_dt >= event_datetime:
                continue

            country = event.xpath(".//td/span/@title").extract_first()

            importance_label = event.xpath(".//td[@class='left textNum sentiment noWrap']/@data-img_key")\
                .extract_first()

            if country not in self.countries or importance_label not in self.importance:
                continue

            if not importance_label:
                logging.warning("Empty importance label for: {} {}".format(country, datetime_str))
                continue

            event_name = event.xpath(".//td[@class='left event']/a/text()").extract_first()
            event_name = event_name.strip(' \r\n\t ')
            event_name_regex = re.findall(r"(.*?)(?=.\([a-zA-Z]{3}\))", event_name)

            if event_name_regex:
                event_name = event_name_regex[0].strip()

            # if event_name not in self.event_list:
            #     continue

            actual = event.xpath(".//td[contains(@id, 'eventActual')]/text()").extract_first().strip('%M BK')

            previous = event.xpath(".//td[contains(@id, 'eventPrevious')]/span/text()").extract_first().strip('%M BK')

            forecast = event.xpath(".//td[contains(@id, 'eventForecast')]/text()").extract_first().strip('%M BK')

            if actual == '\xa0':
                continue

            previous_actual_diff = str(float(previous) - float(actual))

            if forecast != '\xa0':
                forecast_actual_diff = str(float(forecast) - float(actual))

            yield {'Scrap_datetime': current_dt_str,
                'Schedule_datetime': datetime_str,
                'Event': event_name,
                'Importance': importance_label[-1],
                'Actual': actual,
                'Previous': previous,
                'Forecast': forecast if forecast != '\xa0' else None,
                'Prev_actual_diff': previous_actual_diff,
                'Forc_actual_diff': forecast_actual_diff if forecast != '\xa0' else None}



class CrawlerScript(Process):
    def __init__(self, countries, importance, event_list, current_dt, server, topic):

        Process.__init__(self)

        self.countries = countries
        self.importance = importance
        self.event_list = event_list
        self.current_dt = current_dt
        self.server = server
        self.topic = topic

        self.crawler = Crawler(
          EconomicIndicatorsSpiderSpider,
          settings={
            'USER_AGENT': user_agent
          }
        )

        self.crawler.signals.connect(reactor.stop, signal=scrapy_signals.spider_closed)

    def run(self):
        self.crawler.crawl(self.countries, self.importance, self.event_list, self.current_dt,
            self.server, self.topic)
        reactor.run()


def run_indicator_spider(countries, importance, event_list, current_dt, server, topic):

    crawler = CrawlerScript(countries, importance, event_list, current_dt, server, topic)

    # the script will block here until the crawling is finished
    crawler.start()
    crawler.join()


