# -*- coding: utf-8 -*-
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from config import user_agent, time_zone
from datetime import datetime
import logging


class EconomicIndicatorsSpiderSpider(Spider):

    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['http://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.IndicatorCollectorPipeline': 100
        }
    }

    def __init__(self, countries, importance,  event_list, current_dt):

        super(EconomicIndicatorsSpiderSpider, self).__init__()

        self.countries = countries
        self.importance = ['bull' + x for x in importance]
        self.event_list = event_list
        self.current_dt = current_dt

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")

        for event in events:

            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()
            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])

            # Return only events that passed
            # if not self.current_dt >= event_datetime:
            #     continue

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

            if event_name not in self.event_list:
                continue

            print('EVENT NAME: ', event_name)

            actual = event.xpath(".//td[contains(@id, 'eventActual')]/text()").extract_first().strip('%M BK')

            previous = event.xpath(".//td[contains(@id, 'eventPrevious')]/span/text()").extract_first().strip('%M BK')

            forecast = event.xpath(".//td[contains(@id, 'eventForecast')]/text()").extract_first().strip('%M BK')

            if actual == '\xa0':
                continue

            previous_actual_diff = str(float(previous) - float(actual))

            if forecast != '\xa0':
                forecast_actual_diff = str(float(forecast) - float(actual))

            yield {'Datetime': datetime_str,
                'Event': event_name,
                'Importance': importance_label[-1],
                'Actual': actual,
                'Previous': previous,
                'Forecast': forecast if forecast != '\xa0' else None,
                'Prev_actual_diff': previous_actual_diff,
                'Forc_actual_diff': forecast_actual_diff if forecast != '\xa0' else None}


indicators = []


class IndicatorCollectorPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        indicators.append(item)


class ItemsCrawler:
    def __init__(self):

        self.crawler = CrawlerProcess({
            'USER_AGENT': user_agent,
            'LOG_LEVEL': 'INFO'
        })

    def get_indicators(self, countries, importance, event_list, current_dt, start_crawler=False):

        self.crawler.crawl(EconomicIndicatorsSpiderSpider,
              countries=countries, importance=importance, event_list=event_list, current_dt=current_dt)

        # CrawlerProcess can be started only once
        if start_crawler:
            self.crawler.start()

        return indicators

