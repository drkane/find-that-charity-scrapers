# -*- coding: utf-8 -*-
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class ElasticSearchPipeline(object):

    def __init__(self, es_url, es_index, es_type, stats):
        self.es_url = es_url
        self.es_index = es_index
        self.es_type = es_type
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            es_url=crawler.settings.get('ES_URL', 'http://localhost:9200'),
            es_index=crawler.settings.get('ES_INDEX', 'charitysearch'),
            es_type=crawler.settings.get('ES_TYPE', 'organisation'),
            stats=crawler.stats,
        )

    def open_spider(self, spider):
        self.client = Elasticsearch(self.es_url)
        if not self.client.ping():
            raise ValueError("Elasticsearch connection failed")
        self.records = []

    def close_spider(self, spider):
        logging.info("[elasticsearch] saving %s records to %s index", len(self.records), self.es_index)
        self.stats.set_value('elasticsearch/attempted_items', len(self.records))
        results = bulk(self.client, self.records, raise_on_error=False)
        logging.info("[elasticsearch] saved %s records to %s index", results[0], self.es_index)
        logging.info("[elasticsearch] %s errors reported", len(results[1]))
        self.stats.set_value('elasticsearch/indexed_items', results[0])
        self.stats.set_value('elasticsearch/errors', len(results[1]))

    def process_item(self, item, spider):
        es_item = dict(item)
        es_item["_index"] = self.es_index
        es_item["_type"] = self.es_type
        es_item["_op_type"] = "index"
        es_item["_id"] = item["id"]
        del es_item["id"]
        self.records.append(es_item)
        return item
