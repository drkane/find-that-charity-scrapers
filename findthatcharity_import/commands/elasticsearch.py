from __future__ import print_function
import logging

from elasticsearch import Elasticsearch
from scrapy.commands import ScrapyCommand

from ..items import Organisation, Source

class Command(ScrapyCommand):

    requires_project = True

    def short_desc(self):
        return "Manage elasticsearch indices"

    def run(self, args, opts):

        print(args)

        if self.settings.get("ES_URL") is None:
            return

        # @TODO: add a method which sets all the current records from a spider to inactive (or deletes them)

        es = Elasticsearch(self.settings.get("ES_URL"))
        if not es.ping():
            es = None
            raise ValueError("Elasticsearch connection failed")
        
        for i in [Organisation, Source]:
            index_name = i.__name__.lower()
            if not es.indices.exists(index=index_name):
                if hasattr(i, "es_mapping"):
                    es.indices.create(index_name, {
                        "mappings": i.es_mapping()
                    })
                else:
                    es.indices.create(index_name, {})
                logging.info("[elasticsearch] created index '%s'", index_name)
            logging.info("[elasticsearch] index '%s' already exists", index_name)
