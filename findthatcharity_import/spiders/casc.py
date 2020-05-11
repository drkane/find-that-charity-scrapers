# -*- coding: utf-8 -*-
import datetime
import hashlib

import scrapy

from .base_scraper import BaseScraper
from ..items import Link, Organisation, Source

class CascSpider(BaseScraper):
    name = 'casc'
    allowed_domains = ['raw.githubusercontent.com']
    start_urls = [
        "https://raw.githubusercontent.com/ThreeSixtyGiving/cascs/master/cascs.csv",
        "https://raw.githubusercontent.com/ThreeSixtyGiving/cascs/master/casc_company_house.csv",
    ]
    org_id_prefix = "GB-CASC"
    id_field = "id"
    source = {
        "title": "Community amateur sports clubs (CASCs) registered with HMRC",
        "description": "Check which sports clubs are registered with HMRC as community amateur sports clubs. Processed by 360Giving",
        "identifier": "casc",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "HMRC",
            "website": "https://www.gov.uk/government/organisations/hm-revenue-customs",
        },
        "distribution": [
            {
                "downloadURL": "https://github.com/threesixtygiving/cascs",
                "accessURL": "https://www.gov.uk/government/publications/community-amateur-sports-clubs-casc-registered-with-hmrc--2",
                "title": "Government organisations on GOV.UK register"
            }
        ],
    }

    def start_requests(self):

        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()

        return [
            scrapy.Request(self.start_urls[0], callback=self.parse_csv),
            scrapy.Request(self.start_urls[1], callback=self.parse_csv),
        ]

    def parse_row(self, record):

        record = self.clean_fields(record)
        if "casc_orgid" in record.keys():
            return self.parse_row_coyno(record)
        else:
            return self.parse_row_main(record)
    
    def parse_row_coyno(self, record):
        return Link(**{
            "organisation_id_a": record['casc_orgid'],
            "organisation_id_b": record['ch_orgid'],
            "source": self.source["identifier"]
        })

    def parse_row_main(self, record):

        address = dict(enumerate([v.strip() for v in record["address"].split(",", maxsplit=2)]))

        return Organisation(**{
            "id": record["id"],
            "name": record["name"],
            "charityNumber": None,
            "companyNumber": None,
            "streetAddress": address.get(0),
            "addressLocality": address.get(1),
            "addressRegion": address.get(2),
            "addressCountry": None,
            "postalCode": self.parse_postcode(record['postcode']),
            "telephone": None,
            "alternateName": [],
            "email": None,
            "description": None,
            "organisationType": ["Community Amateur Sports Club", "Sports Club"],
            "organisationTypePrimary": ["Community Amateur Sports Club"],
            "url": None,
            "location": [],
            "latestIncome": None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": None,
            "dateRemoved": None,
            "active": True,
            "parent": None,
            "orgIDs": [record["id"]],
            "source": self.source["identifier"],
        })
