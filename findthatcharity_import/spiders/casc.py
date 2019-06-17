# -*- coding: utf-8 -*-
import datetime
import hashlib

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source

class CascSpider(BaseScraper):
    name = 'casc'
    allowed_domains = ['gov.uk']
    start_urls = ["https://www.gov.uk/government/publications/community-amateur-sports-clubs-casc-registered-with-hmrc--2"]
    org_id_prefix = "GB-CASC"
    source = {
        "title": "Community amateur sports clubs (CASCs) registered with HMRC",
        "description": "Check which sports clubs are registered with HMRC as community amateur sports clubs as at April 2018.",
        "identifier": "casc",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "HMRC",
            "website": "https://www.gov.uk/government/organisations/hm-revenue-customs",
        },
        "distribution": [],
    }

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback=self.find_files)]

    def find_files(self, response):
        self.rowcount = 0
        for list_page in response.css(".attachment-details h2"):
            yield response.follow(list_page.css("a::attr(href)").extract_first(), callback=self.parse)

        self.source["modified"] = datetime.datetime.now().isoformat()
        yield Source(**self.source)

    def parse(self, response):
        """
        Parse an individual page on the HMRC website
        """

        self.source["distribution"] = [{
            "downloadURL": response.url,
            "accessURL": self.start_urls[0],
            "title": "Community amateur sports clubs (CASCs) registered with HMRC - {}".format(
                response.css("h1.gem-c-title__text::text").extract_first().strip()
            )
        }]
        yield Source(**self.source)

        for table in response.css(".govspeak > table"):
            for row in table.css("tbody > tr"):
                cells = row.css("td::text").extract()
                self.rowcount += 1

                if self.settings.getbool("DEBUG_ENABLED") and self.rowcount > self.settings.getint("DEBUG_ROWS", 100):
                    break

                address = dict(enumerate([v.strip() for v in cells[1].split(",", maxsplit=2)]))

                yield Organisation(**{
                    "id": self.get_org_id(cells),
                    "name": cells[0],
                    "charityNumber": None,
                    "companyNumber": None,
                    "streetAddress": address.get(0),
                    "addressLocality": address.get(1),
                    "addressRegion": address.get(2),
                    "addressCountry": None,
                    "postalCode": self.parse_postcode(cells[2]),
                    "telephone": None,
                    "alternateName": [],
                    "email": None,
                    "description": None,
                    "organisationType": ["Sports Club", "Community Amateur Sports Club"],
                    "url": None,
                    "location": [],
                    "latestIncome": None,
                    "dateModified": datetime.datetime.now(),
                    "dateRegistered": None,
                    "dateRemoved": None,
                    "active": True,
                    "parent": None,
                    "orgIDs": [self.get_org_id(cells)],
                    "sources": [self.source["identifier"]],
                })

    def get_org_id(self, record):
        """
        CASCs don't come with a ID, so we're creating a dummy one.

        This is defined by:

        1. Put together the name of the club + the postcode (or the string None if there is no postcode)
        2. Take the MD5 hash of the utf8 representation of this string
        3. Use the first 8 characters of the hexdigest of this hash
        """

        def hash_id(w):
            return hashlib.md5(w.encode("utf8")).hexdigest()[0:8]

        return "-".join([
            self.org_id_prefix,
            hash_id(str(record[0])+str(record[2]))
        ])
