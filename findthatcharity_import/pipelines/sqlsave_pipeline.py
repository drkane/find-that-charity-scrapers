# -*- coding: utf-8 -*-
import json
import logging
import uuid
import datetime

from sqlalchemy import create_engine, and_
from sqlalchemy.exc import InternalError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import insert, delete
from sqlalchemy.dialects import postgresql, mysql
import scrapy
from scrapy import signals
from scrapy.utils.serialize import ScrapyJSONEncoder

from ..db import metadata, tables

class SQLSavePipeline(object):

    def __init__(self, db_uri, chunk_size, stats):
        self.db_uri = db_uri
        self.chunk_size = chunk_size
        self.stats = stats
        self.spider_name = None
        self.crawl_id = uuid.uuid4().hex
        
        # logging.basicConfig()
        # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(
            db_uri=crawler.settings.get('DB_URI'),
            chunk_size=int(crawler.settings.get('DB_CHUNK', 5000)),
            stats=crawler.stats,
        )
        crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        if hasattr(self, "conn"):
            this_tables = item.to_tables()
            for t in this_tables:
                if not type(this_tables[t]) == list:
                    this_tables[t] = [this_tables[t]]
                self.records[t].extend(this_tables[t])
                self.record_count += 1

            if self.record_count > self.chunk_size:
                self.commit_records(spider)
                pass

        return item

    def commit_records(self, spider):
        spider.logger.info("Commiting {} records".format(getattr(self, "record_count", 0)))
        self.save_stats()
        
        for t in getattr(self, "records", {}):
            table = self.tables[t]
            cols = [c.name for c in table.columns]
            pks = [c.name for c in table.primary_key]
            vals = []

            if self.engine.name == 'postgresql':
                insert_statement = postgresql.insert(table)
                upsert_statement = insert_statement.on_conflict_do_update(
                    constraint=table.primary_key,
                    set_={c: insert_statement.excluded.get(c) for c in cols}
                )
            elif self.engine.name == 'mysql':
                insert_statement = mysql.insert(table)
                upsert_statement = insert_statement.on_duplicate_key_update(
                    **{c: insert_statement.excluded.get(c) for c in cols}
                )
            elif self.engine.name == 'sqlite':
                upsert_statement = insert(table).prefix_with("OR REPLACE")
                    
            for r in self.records[t]:
                if "scrape_id" in cols:
                    r['scrape_id'] = self.crawl_id
                if "spider" in cols:
                    r['spider'] = self.spider_name

                if self.engine.name == 'postgresql':
                    vals.append({c: r.get(c) for c in cols})
                else:
                    vals.append({c: str(r.get(c)) if type(r.get(c)) in (
                        list, dict) else r.get(c) for c in cols})
            if vals:
                self.conn.execute(upsert_statement, vals)

        self.records = {t: [] for t in self.tables}
        self.record_count = 0


    def open_spider(self, spider):
        self.spider_name = spider.name
        if self.db_uri:
            self.engine = create_engine(self.db_uri)
            Session = sessionmaker(bind=self.engine)
            self.conn = Session()

            self.tables = {t.name: t for t in tables.values()}
            self.records = {t: [] for t in self.tables}
            metadata.create_all(self.engine)

            # delete any existing records from the tables
            for t, table in self.tables.items():
                cols = [c.name for c in table.columns]
                if "scrape_id" in cols and "spider" in cols:
                    self.conn.execute(
                        delete(table).where(and_(
                            table.c.scrape_id != self.crawl_id,
                            table.c.spider == self.spider_name
                        ))
                    )

            # do any tasks before the spider is run
            # if hasattr(spider, "name"):
            #     if self.conn.engine.dialect.has_table(self.conn.engine, tables["organisation"].name):
            #         self.conn.execute(
            #             tables["organisation"].update()\
            #                 .values(active=False)\
            #                 .where(
            #                     tables["organisation"].c.id == tables["organisation_sources"].select(
            #                     ).with_only_columns([
            #                         tables["organisation_sources"].c.organisation_id
            #                     ]).where(
            #                         tables["organisation_sources"].c.source_id == getattr(spider, "name")
            #                     )
            #                 )
            #         )

            self.commit_records(spider)

    def spider_closed(self, spider, reason):
        if hasattr(self, "conn"):
            self.commit_records(spider)
            self.conn.commit()
            self.conn.close()

    def save_stats(self):
        stats = self.stats.get_stats()

        status = stats.get('finish_reason')
        if not stats.get('finish_time'):
            status = 'in_progress'
        elif stats.get('log_count/ERROR', 0) > 0 or stats.get('item_scraped_count', 0) == 0:
            status = "errors"

        to_save = {
            "id": self.crawl_id,
            "spider": self.spider_name,
            "stats": json.dumps(stats, cls=ScrapyJSONEncoder),
            "finish_reason": status,
            "errors": stats.get('log_count/ERROR', 0),
            "items": stats.get('item_scraped_count', 0),
            "start_time": stats.get('start_time', datetime.datetime.utcnow()),
            "finish_time": stats.get('finish_time'),
        }
        self.records['scrape'].append(to_save)
