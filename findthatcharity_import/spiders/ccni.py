# -*- coding: utf-8 -*-
import datetime
import io
import csv

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation

class CCNISpider(BaseScraper):
    name = 'ccni'
    allowed_domains = ['charitycommissionni.org.uk', 'gist.githubusercontent.com']
    start_urls = [
        "http://www.charitycommissionni.org.uk/charity-search/?q=&include=Linked&include=Removed&exportCSV=1",
        "https://gist.githubusercontent.com/BobHarper1/2687545c562b47bc755aef2e9e0de537/raw/ac052c33fd14a08dd4c2a0604b54c50bc1ecc0db/ccni_extra"
    ]
    org_id_prefix = "GB-NIC"
    id_field = "Reg charity number"
    date_fields = ["Date registered", "Date for financial year ending"]
    date_format = {
        "Date registered": "%d/%m/%Y",
        "Date for financial year ending": "%d %B %Y"
    }
    source = {
        "title": "Charity Commission for Northern Ireland charity search",
        "description": "",
        "identifier": "ccni",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Charity Commission for Northern Ireland",
            "website": "https://www.education-ni.gov.uk/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "http://www.charitycommissionni.org.uk/charity-search/",
                "title": "Charity Commission for Northern Ireland charity search"
            }
        ],
    }

    def start_requests(self):
        return [scrapy.Request(self.start_urls[1], callback=self.download_file)]

    def download_file(self, response):

        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()

        # set up extra names db first
        self.extra_names = {}
        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                regno = row["Charity_number"].strip()
                if regno not in self.extra_names:
                    self.extra_names[regno] = []
                self.extra_names[regno].append(row["Other_names"].strip())

        return [scrapy.Request(self.start_urls[0], callback=self.parse_csv)]

    def parse_row(self, record):

        record = self.clean_fields(record)

        address, postcode = self.split_address(record.get("Public address", ""))

        org_types = [
            "Registered Charity",
            "Registered Charity (Northern Ireland)",
        ]
        org_ids = [self.get_org_id(record)]
        coyno = self.parse_company_number(record.get("Company number"))
        if coyno:
            org_types.append("Registered Company")
            org_ids.append("GB-COH-{}".format(coyno))

        return Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("Charity name").replace("`", "'"),
            "charityNumber": "NI{}".format(record.get(self.id_field)),
            "companyNumber": coyno,
            "streetAddress": address[0],
            "addressLocality": address[1],
            "addressRegion": address[2],
            "addressCountry": "Northern Ireland",
            "postalCode": postcode,
            "telephone": record.get("Telephone"),
            "alternateName": self.extra_names.get(record.get(self.id_field), []),
            "email": record.get("Email"),
            "description": None,
            "organisationType": org_types,
            "url": self.parse_url(record.get("Website")),
            "location": [],
            "latestIncome": int(record["Total income"]) if record.get("Total income") else None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": record.get("Date registered"),
            "dateRemoved": None,
            "active": record.get("Status") != "Removed",
            "parent": None,
            "orgIDs": org_ids,
            "sources": [self.source],
        })

    def parse_company_number(self, coyno):
        if not coyno:
            return None

        coyno = coyno.strip()
        if coyno == "" or int(coyno) == 0 or int(coyno) == 999999:
            return None

        if coyno.isdigit():
            return "NI" + coyno.rjust(6, "0")

        return coyno
