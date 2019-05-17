# -*- coding: utf-8 -*-
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class ElasticSearchPipeline():

    def __init__(self, es_url, es_index, es_type, es_bulk_limit, stats):
        self.es_url = es_url
        self.es_index = es_index
        self.es_type = es_type
        self.es_bulk_limit = es_bulk_limit
        self.stats = stats
        self.client = None
        self.records = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            es_url=crawler.settings.get('ES_URL'),
            es_index=crawler.settings.get('ES_INDEX', 'charitysearch'),
            es_type=crawler.settings.get('ES_TYPE', 'organisation'),
            es_bulk_limit=crawler.settings.get('ES_BULK_LIMIT', 500),
            stats=crawler.stats,
        )

    def open_spider(self, spider):

        if self.es_url is None:
            return

        # @TODO: add a method which sets all the current records from a spider to inactive (or deletes them)

        self.client = Elasticsearch(self.es_url)
        if not self.client.ping():
            self.client = None
            raise ValueError("Elasticsearch connection failed")
        self.records = []

    def close_spider(self, spider):
        if self.client is None:
            return
        self.save_records()

    def save_records(self):
        self.stats.inc_value('elasticsearch/attempted_items', len(self.records))

        results = bulk(self.client, self.records, raise_on_error=False, chunk_size=self.es_bulk_limit)

        logging.info("[elasticsearch] saved %s records to %s index", results[0], self.es_index)
        self.stats.inc_value('elasticsearch/indexed_items', results[0])
        if results[1]:
            logging.info("[elasticsearch] %s errors reported", len(results[1]))
            logging.info("First 5 errors")
            for e in results[1][0:5]:
                logging.info(e)
            self.stats.inc_value('elasticsearch/errors', len(results[1]))

        self.records = []

    def process_item(self, item, spider):

        if self.client is None:
            return item

        # check for a to_elasticsearch method on the item
        if hasattr(item, "to_elasticsearch") and callable(item.to_elasticsearch):
            es_item = item.to_elasticsearch(self.es_index, self.es_type)
            es_item["_index"] = es_item.get("_index", self.es_index)
            es_item["_type"] = es_item.get("_type", self.es_type)
            es_item["_op_type"] = es_item.get("_op_type", "index")
        else:
            es_item = dict(item)
            es_item["_index"] = self.es_index
            es_item["_type"] = self.es_type
            es_item["_op_type"] = "index"
            es_item["_id"] = item.get("id")
            del es_item["id"]

        if not es_item.get("_id"):
            logging.warning("[elasticsearch] Cannot save as no ID provided for item")
            return item

        self.records.append(es_item)

        if len(self.records) >= self.es_bulk_limit:
            self.save_records()


        return item
