# -*- coding: utf-8 -*-
import logging

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

class MongoDBPipeline():

    def __init__(self, mongo_uri, mongo_db, mongo_collection, mongo_bulk_limit, stats):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_bulk_limit = mongo_bulk_limit
        self.stats = stats
        self.client = None
        self.records = {} # dict with mongodb collections as keys and a list of items to insert

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB', 'charitysearch'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION', 'organisation'),
            mongo_bulk_limit=crawler.settings.get('MONGO_BULK_LIMIT', 50000),
            stats=crawler.stats,
        )

    def open_spider(self, spider):

        if not self.mongo_uri:
            return

        # @TODO: add a method which sets all the current records from a spider to inactive (or deletes them)
        self.client = MongoClient(self.mongo_uri)
        self.client.server_info()
        self.records = {}

    def close_spider(self, spider):
        if self.client is None:
            return
        self.save_records()
        self.client.close()

    def save_records(self):

        for collection, records in self.records.items():
            self.stats.inc_value('mongodb/attempted_items', len(records))
            bulk = self.client[self.mongo_db][collection].initialize_unordered_bulk_op()
            for r in records:
                bulk.find({"_id": r["_id"]}).upsert().replace_one(r)
            try:
                results = bulk.execute()
            except BulkWriteError as bwe:
                results = bwe.details

            for i in ['Inserted', 'Matched', 'Modified', 'Removed', 'Upserted']:
                if results.get('n'+i, 0) > 0:
                    logging.info("[mongodb] %s %s records in %s collection", i, results.get('n'+i, 0), collection)
                    self.stats.inc_value('mongodb/{}_items'.format(i.lower()), results.get('n'+i, 0))
            if results['writeErrors']:
                logging.info("[mongodb] %s errors reported", len(results['writeErrors']))
                logging.info("First 5 errors")
                for e in results['writeErrors'][0:5]:
                    logging.info(e['errmsg'])
                self.stats.inc_value('mongodb/errors', len(results['writeErrors']))

        self.records = {}

    def process_item(self, item, spider):

        if self.client is None:
            return item

        # check for a to_mongodb method on the item
        if hasattr(item, "to_mongodb") and callable(item.to_mongodb):
            collection, mongo_item = item.to_mongodb()
        else:
            es_item = dict(item)
            es_item["_id"] = item["id"]
            del es_item["id"]
            collection = None

        if not collection:
            collection = self.mongo_collection
        if collection not in self.records:
            self.records[collection] = []
        self.records[collection].append(mongo_item)

        for collection, records in self.records.items():
            if len(records) >= self.mongo_bulk_limit:
                self.save_records()

        return item
