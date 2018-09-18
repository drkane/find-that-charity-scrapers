import io
import csv
import datetime
import re

import scrapy

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

class BaseScraper(scrapy.Spider):

    date_format = DEFAULT_DATE_FORMAT

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
        for f in record.keys():
            # clean blank values
            if record[f].strip() == "":
                record[f] = None

            # clean date fields
            elif f in self.date_fields:
                date_format = self.date_format
                if isinstance(date_format, dict):
                    date_format = date_format.get(f, DEFAULT_DATE_FORMAT)

                try:
                    if record.get(f):
                        record[f] = datetime.datetime.strptime(record.get(f).strip(), date_format)
                except ValueError:
                    record[f] = None

            # strip string fields
            elif isinstance(record[f], str):
                record[f] = record[f].strip()
        return record

    def slugify(self, value):
        value = value.lower()
        value = re.sub(r'\([0-9]+\)', "_", value).strip("_") # replace values in brackets
        value = re.sub(r'[^0-9A-Za-z]+', "_", value).strip("_") # replace any non-alphanumeric characters
        return value

    def parse_company_number(self, coyno):
        if not coyno:
            return None
        
        coyno = coyno.strip()
        if coyno == "":
            return None

        if coyno.isdigit():
            return coyno.rjust(8, "0")

        return coyno
