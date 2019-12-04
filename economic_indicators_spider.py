# -*- coding: utf-8 -*-
import scrapy
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item
from config import user_agent, time_zone
from datetime import datetime, timedelta
from pytz import timezone
import logging


class EventItem(Item):
    event = scrapy.Field()


class EventsSchedule(Spider):

    name = 'events_schedule'
    allowed_domains = ['www.investing.com']
    start_urls = ['http://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.ScheduleCollectorPipeline': 100
        }
    }

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")
        item = EventItem()

        for event in events:
            # Extract event datetime in format: '2019/11/26 16:30:00'
            event_str = event.xpath(".//@data-event-datetime").extract_first()
            # Convert string to datetime object
            event_dt = datetime.strptime(event_str, "%Y/%m/%d %H:%M:%S")
            event_dt = event_dt.replace(tzinfo=timezone('US/Eastern'))
            item['event'] = event_dt
            yield item


# list to collect all items
schedule_items = []

# Pipeline that adds items to the list
class ScheduleCollectorPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        schedule_items.append(item['event'])


class EconomicIndicatorsSpiderSpider(Spider):

    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['http://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.IndicatorCollectorPipeline': 100
        }
    }

    def __init__(self, countries, importance,  current_dt, freq):

        super(EconomicIndicatorsSpiderSpider, self).__init__()

        self.countries = countries
        self.importance = ['bull' + x for x in importance]
        self.current_dt = current_dt
        self.freq = freq

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")

        for event in events:

            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()
            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])

            if not (self.current_dt >= event_datetime and self.current_dt <= event_datetime + timedelta(seconds=self.freq)):
                print('CONTINUE')
                continue

            print('NOT CONTINUE, TIME OK')

            country = event.xpath(".//td/span/@title").extract_first()

            importance_label = event.xpath(".//td[@class='left textNum sentiment noWrap']/@data-img_key")\
                .extract_first()

            if country not in self.countries or importance_label not in self.importance:
                continue

            if not importance_label:
                logging.warning("Empty importance label for: {} {}".format(country, datetime_str))
                continue

            event_name = event.xpath(".//td[@class='left event']/a/text()").extract_first()
            event_name = event_name.strip('\r\n\t ')

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

    def get_indicators(self, countries, importance, current_dt, freq, start_crawler=False):

        self.crawler.crawl(EconomicIndicatorsSpiderSpider,
              countries=countries, importance=importance, current_dt=current_dt, freq=freq)

        if start_crawler:
            self.crawler.start()

        return indicators

    def get_schedule(self, start_crawler=False):

        self.crawler.crawl(EventsSchedule)

        if start_crawler:
            self.crawler.start()

        return schedule_items

