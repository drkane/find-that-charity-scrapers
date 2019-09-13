# -*- coding: utf-8 -*-
import datetime
import io
import csv

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source


# List of providers from Hesa
class HesaSpider(BaseScraper):
    name = 'hesa'
    allowed_domains = ['hesa.ac.uk']
    start_urls = [
        "https://www.hesa.ac.uk/support/providers",
        "https://raw.githubusercontent.com/drkane/charity-lookups/master/university-charity-number.csv",
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

    org_types = {
        "HEI": ["University", "Higher Education"],
        "FEC": ["Further Education College", "Higher Education"],
        "AP": ["Alternative Provider", "Higher Education"],
    }

    def start_requests(self):

        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        return [scrapy.Request(self.start_urls[1], callback=self.charity_number_lookup)]


    def charity_number_lookup(self, response):
        """
        Lookup university <> charity number (as Org ID)
        """

        self.unichar = {}
        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                self.unichar[row["HESA ID"].rjust(4, '0')] = row["OrgID"]

        self.logger.info("Imported University charity numbers")
        return scrapy.Request(self.start_urls[0], callback=self.get_rows)

    def get_rows(self, response):
        for row in response.css("table#heps-table tbody tr"):
            cells = row.css("td::text").extract()

            orgids = [
                "-".join([self.org_id_prefix, str(cells[1])]),
                "-".join(["GB-UKPRN", str(cells[0])]),
            ]

            if cells[1] in self.unichar:
                orgids.append(self.unichar[cells[1]])

            yield Organisation(**{
                "id": "-".join([self.org_id_prefix, str(cells[1])]),
                "name": cells[2].strip(),
                "charityNumber": None,
                "companyNumber": None,
                "streetAddress": None,
                "addressLocality": None,
                "addressRegion": None,
                "addressCountry": None,
                "postalCode": None,
                "telephone": None,
                "alternateName": [],
                "email": None,
                "description": None,
                "organisationType": self.org_types.get(cells[4].strip(), cells[4].strip()),
                "url": None,
                "location": [],
                "latestIncome": None,
                "dateModified": datetime.datetime.now(),
                "dateRegistered": None,
                "dateRemoved": None,
                "active": True,
                "parent": None,
                "orgIDs": orgids,
                "source": self.source["identifier"],
            })

        self.source["modified"] = datetime.datetime.now().isoformat()
        yield Source(**self.source)
