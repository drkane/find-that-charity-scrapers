from __future__ import print_function
import logging

from elasticsearch import Elasticsearch
from scrapy.commands import ScrapyCommand

from ..items import Organisation, Source

class Command(ScrapyCommand):

    requires_project = True

    def short_desc(self):
        return "Manage elasticsearch indices"

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("-r", "--reset", action="store_true",
                          help="Reset the index before creating (WARNING: WILL DELETE DATA)")


    def run(self, args, opts):

        if self.settings.get("ES_URL") is None:
            logging.info("[elasticsearch] Can't connect - no connection information present")
            logging.info("[elasticsearch] Use the ES_URL setting to connect to elasticsearch")
            return

        es = Elasticsearch(self.settings.get("ES_URL"))
        if not es.ping():
            es = None
            raise ValueError("Elasticsearch connection failed")
        
        for i in [Organisation, Source]:
            index_name = i.__name__.lower()

            if opts.get('reset') and es.indices.exists(index=index_name):
                confirm = input("This will delete index \"{}\" - are you sure? [y]es/[n]o".format(index_name))
                if confirm[0:1].lower()=="y":
                    es.indices.delete(index=index_name)

            if not es.indices.exists(index=index_name):
                if hasattr(i, "es_mapping"):
                    es.indices.create(index_name, {
                        "mappings": i.es_mapping()
                    })
                else:
                    es.indices.create(index_name, {})
                logging.info("[elasticsearch] created index '%s'", index_name)
            logging.info("[elasticsearch] index '%s' already exists", index_name)
