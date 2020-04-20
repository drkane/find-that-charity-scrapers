import csv

from scrapy.exporters import CsvItemExporter


def item_type(item):
    return type(item).__name__.replace('Item','').lower()

class MultiCSVPipeline(object):
    """Distribute items across multiple CSV files according to the item type"""

    def open_spider(self, spider):
        self.source = spider.source['identifier']
        self.item_to_exporter = {}

    def close_spider(self, spider):
        for exporter in self.item_to_exporter.values():
            exporter["exporter"].finish_exporting()
            exporter["file"].close()

    def _exporter_for_item(self, item):
        itemtype = item_type(item)
        if itemtype not in self.item_to_exporter:
            fields = None
            existing_items = []
            with open('data/{}.csv'.format(itemtype), 'r') as f:
                reader = csv.DictReader(f)
                if itemtype=="organisation":
                    existing_items = [row for row in reader if row["source"] != self.source]
                elif itemtype=="source":
                    existing_items = [row for row in reader if row["identifier"] != self.source]
                if existing_items:
                    fields = existing_items[0].keys()
            if not fields:
                fields = list(item.keys())

            f = open('data/{}.csv'.format(itemtype), 'w', newline='')
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(existing_items)
            exporter = MultiCsvItemExporter(f, fields_to_export=fields)
            exporter.start_exporting()
            self.item_to_exporter[itemtype] = {
                "exporter": exporter,
                "file": f,
            }
        return self.item_to_exporter[itemtype]

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item)
        exporter["exporter"].export_item(item)
        return item

class MultiCsvItemExporter(CsvItemExporter):

    def __init__(self, file, include_headers_line=True, join_multivalued=',', **kwargs):
        super().__init__(file, **kwargs)
        # self.stream = file
        self._headers_not_written = False
        self.csv_writer = csv.writer(file, **self._kwargs)
