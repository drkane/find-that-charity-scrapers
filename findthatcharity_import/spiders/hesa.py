# -*- coding: utf-8 -*-
import datetime

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source


# List of providers from Hesa
class HesaSpider(BaseScraper):
    name = 'hesa'
    allowed_domains = ['hesa.ac.uk']
    start_urls = [
        "https://www.hesa.ac.uk/support/providers"
    ]
    org_id_prefix = "GB-HESA"
    source = {
        "title": "Higher Education Statistics Agency",
        "description": "Higher Education Statistics Agency - we are the experts in UK higher education data and analysis, and the designated data body for England. We collect, process, and publish data about higher education (HE) in the UK. As the trusted source of HE data and analysis, we play a key role in supporting and enhancing the competitive strength of the sector.",
        "identifier": "hesa",
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "license_name": "Creative Commons Attribution 4.0 International Licence",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "HESA",
            "website": "https://www.hesa.ac.uk/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "",
                "title": "HESA - Higher education providers"
            }
        ],
    }

    def start_requests(self):

        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        return [scrapy.Request(self.start_urls[0], callback=self.get_rows)]

    def get_rows(self, response):
        for row in response.css("table#heps-table tbody tr"):
            cells = row.css("td::text").extract()
            yield Organisation(**{
                "id": "-".join([self.org_id_prefix, str(cells[1])]),
                "name": cells[2],
                "charityNumber": None,
                "companyNumber": None,
                "streetAddress": None,
                "addressLocality": None,
                "addressRegion": None,
                "addressCountry": None,
                "postalCode": None,
                "telephone": None,
                "alternateName": None,
                "email": None,
                "description": None,
                "organisationType": ["Higher Education"],
                "url": None,
                "location": [],
                "latestIncome": None,
                "dateModified": datetime.datetime.now(),
                "dateRegistered": None,
                "dateRemoved": None,
                "active": True,
                "parent": None,
                "orgIDs": [
                    "-".join([self.org_id_prefix, str(cells[1])]),
                    "-".join(["GB-UKPRN", str(cells[0])]),
                ],
                "sources": [self.source["identifier"]],
            })
