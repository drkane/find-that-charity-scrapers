# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, and_
from sqlalchemy.exc import InternalError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import insert
from sqlalchemy.dialects import postgresql, mysql
from ..db import metadata, tables
import scrapy 
import logging

class SQLSavePipeline(object):

    def __init__(self, db_uri, chunk_size):
        self.db_uri = db_uri
        self.chunk_size = chunk_size
        
        # logging.basicConfig()
        # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            db_uri=crawler.settings.get('DB_URI'),
            chunk_size=int(crawler.settings.get('DB_CHUNK', 1000)),
        )

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
        if self.db_uri:
            self.engine = create_engine(self.db_uri)
            self.conn = self.engine.connect()

            self.tables = {t.name: t for t in tables.values()}
            metadata.create_all(self.engine)

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

    def close_spider(self, spider):
        if hasattr(self, "conn"):
            self.commit_records(spider)

            self.conn.close()
