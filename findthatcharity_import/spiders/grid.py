# -*- coding: utf-8 -*-
import datetime
import zipfile
import json
import io

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source


# Scrape from Global Research Identifiers Database
class GridSpider(BaseScraper):
    name = 'grid'
    allowed_domains = ['grid.ac', 'doi.org', 'figshare.com']
    start_urls = [
        "https://www.grid.ac/downloads"
    ]
    org_id_prefix = "XI-GRID"
    id_field = "id"
    source = {
        "title": "Global Research Identifiers Database",
        "description": '''The Global Research Identifiers Database collects information on research institutions and assigns them a unique identifier.

It draws on information from funding datasets, and claims over 90% coverage of institutions.

It records information on the nature of the research organisation, covering companies, education establishments, healthcare, non-profits, government and other entity types.

GRID also records parent-child relationships between entities, and 'related relationships' for cross-linkages.

It includes cross-linkages to a range of other identifier sources.''',
        "identifier": "grid",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "license_name": "Creative Commons Public Domain 1.0 International licence",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Digital Science",
            "website": "http://www.digital-science.com/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "https://www.grid.ac/downloads",
                "title": "GRID Download"
            }
        ],
    }
    included_countries = ['GB']

    def start_requests(self):

        return [scrapy.Request(self.start_urls[0], callback=self.find_zip)]

    def find_zip(self, response):
        link = response.xpath('//a[text()="Download latest release"]/@href').extract_first()

        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["distribution"][0]["downloadURL"] = link
        self.source["modified"] = datetime.datetime.now().isoformat()
        
        return [scrapy.Request(response.urljoin(link), callback=self.fetch_zip)]

    def fetch_zip(self, response):
        link = response.xpath('//a[text()="Download"]/@href').extract_first()
        
        return [scrapy.Request(response.urljoin(link), callback=self.process_zip)]
        
    def process_zip(self, response):
        yield Source(**self.source)
        with zipfile.ZipFile(io.BytesIO(response.body)) as z:
            with z.open("grid.json") as gridjson:
                data = json.load(gridjson)
                rowcount = 0
                for k, i in enumerate(data.get("institutes", [])):
                    if self.settings.getbool("DEBUG_ENABLED") and rowcount >= self.settings.getint("DEBUG_ROWS", 100):
                        break

                    # We only want data from certain countries
                    if i.get("addresses", [{}])[0].get("country_code") not in self.included_countries:
                        continue

                    # And we only want certain types of organisation (eg exclude private companies)
                    if "Company" in i.get("types", []):
                        continue

                    rowcount += 1

                    yield self.parse_row(i)

    def parse_row(self, record):

        record = self.clean_fields(record)

        address = []
        for a in ["line_1", "line_2", "line_3"]:
            an = record.get("addresses", [{}])[0].get(a)
            if an and an != "":
                address.append(an)

        url = record.get("links")[0] if record.get("links") else None

        parent = None
        for r in record.get("relationships", [{}]):
            if r.get("type")=="Parent":
                parent = self.org_id_prefix + "-" + r.get("id")

        org = Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("name"),
            "charityNumber": None,
            "companyNumber": None,
            "streetAddress": ", ".join(address),
            "addressLocality": record.get("addresses", [{}])[0].get("city"),
            "addressRegion": record.get("addresses", [{}])[0].get("state"),
            "addressCountry": record.get("addresses", [{}])[0].get("country"),
            "postalCode": record.get("addresses", [{}])[0].get("postcode"),
            "telephone": None,
            "alternateName": record.get("aliases", []) + record.get("acronyms", []),
            "email": record.get("email_address"),
            "description": None,
            "organisationType": record.get("types", []),
            "organisationTypePrimary": record.get("types", [])[0],
            "url": url,
            "location": [],
            "latestIncome": None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": None,
            "dateRemoved": None,
            "active": record.get("status") == "active",
            "parent": parent,
            "orgIDs": [self.get_org_id(record)] + self.get_org_ids(record.get("external_ids", {})),
            "source": self.source["identifier"],
        })

        return org

    def get_org_ids(self, record):
        orgids = []

        for scheme, value in record.items():
            if scheme == 'UKPRN':
                orgids.extend(["GB-UKPRN-{}".format(v) for v in value["all"]])
            elif scheme == 'HESA':
                orgids.extend(["GB-HESA-{}".format(v) for v in value["all"]])
            elif scheme == 'UCAS':
                orgids.extend(["GB-UCAS-{}".format(v) for v in value["all"]])

        return orgids
