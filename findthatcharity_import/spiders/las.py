# -*- coding: utf-8 -*-
import datetime
import io
import csv

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source
from .lae import LA_TYPES

class LocalAuthorityScotlandSpider(BaseScraper):
    name = 'las'
    allowed_domains = ['register.gov.uk']
    start_urls = [
        "https://local-authority-sct.register.gov.uk/records.csv?page-size=5000"
    ]
    org_id_prefix = "GB-LAS"
    id_field = "key"
    date_fields = ["entry-timestamp", "start-date", "end-date"]
    date_format = {
        "entry-timestamp": "%Y-%m-%dT%H:%M:%SZ",
        "start-date": "%Y-%m-%d",
        "end-date": "%Y-%m-%d",
    }
    source = {
        "title": "Local authorites in Scotland register",
        "description": "Local authorities in Scotland",
        "identifier": "las",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Scottish Government",
            "website": "https://gov.scot/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "https://www.registers.service.gov.uk/registers/local-authority-sct/",
                "title": "Local authorites in Scotland register"
            }
        ],
    }

    def start_requests(self):

        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()

        return [scrapy.Request(self.start_urls[0], callback=self.parse_csv)]

    def parse_row(self, record):

        record = self.clean_fields(record)

        org_types = [
            "Local Authority",
            LA_TYPES.get(record.get("local-authority-type"), record.get("local-authority-type")),
        ]
        org_ids = [self.get_org_id(record)]

        locations = []
        # @TODO: map local authority code to GSS to add locations

        return Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("official-name"),
            "charityNumber": None,
            "companyNumber": None,
            "streetAddress": None,
            "addressLocality": None,
            "addressRegion": None,
            "addressCountry": "Scotland",
            "postalCode": None,
            "telephone": None,
            "alternateName": [],
            "email": None,
            "description": None,
            "organisationType": org_types,
            "organisationTypePrimary": "Local Authority",
            "url": None,
            "location": locations,
            "latestIncome": None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": record.get("start-date"),
            "dateRemoved": record.get("end-date"),
            "active": record.get("end-date") is None,
            "parent": None,
            "orgIDs": org_ids,
            "source": self.source["identifier"],
        })
