import io
import csv
import datetime
import re

import scrapy

class BaseScraper(scrapy.Spider):

    def parse_csv(self, response):
        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for k, row in enumerate(csvreader):
                if self.settings.getbool("DEBUG_ENABLED") and k > self.settings.getint("DEBUG_ROWS", 100):
                    break
                yield self.parse_row(row)

    def get_org_id(self, record):
        return "-".join([self.org_id_prefix, str(record.get(self.id_field))])

    def clean_fields(self, record):
        # clean blank values
        for f in record.keys():
            if record[f] == "":
                record[f] = None
            elif f in self.date_fields:
                try:
                    if record.get(f):
                        record[f] = datetime.datetime.strptime(record.get(f).strip(), "%d-%m-%Y")
                except ValueError:
                    record[f] = None
            elif isinstance(record[f], str):
                record[f] = record[f].strip()
        return record

    def slugify(self, value):
        value = value.lower()
        value = re.sub(r'\([0-9]+\)', "_", value).strip("_") # replace values in brackets
        value = re.sub(r'[^0-9A-Za-z]+', "_", value).strip("_") # replace any non-alphanumeric characters
        return value
