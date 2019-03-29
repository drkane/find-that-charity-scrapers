# -*- coding: utf-8 -*-
import datetime
import io
from pyexcel_ods3 import get_data

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, Source, AREA_TYPES

WAL_LAS = {
    "Blaenau Gwent": "W06000019",
    "Bridgend": "W06000013",
    "Caerphilly": "W06000018",
    "Cardiff": "W06000015",
    "Carmarthenshire": "W06000010",
    "Ceredigion": "W06000008",
    "Conwy": "W06000003",
    "Denbighshire": "W06000004",
    "Flintshire": "W06000005",
    "Gwynedd": "W06000002",
    "Isle of Anglesey": "W06000001",
    "Merthyr Tydfil": "W06000024",
    "Monmouthshire": "W06000021",
    "Neath Port Talbot": "W06000012",
    "Newport": "W06000022",
    "Pembrokeshire": "W06000009",
    "Powys": "W06000023",
    "Rhondda Cynon Taf": "W06000016",
    "Swansea": "W06000011",
    "The Vale of Glamorgan": "W06000014",
    "Torfaen": "W06000020",
    "Wrexham": "W06000006",
}

class SchoolsWalesSpider(BaseScraper):
    name = 'schools_wales'
    allowed_domains = ['gov.wales']
    start_urls = ["https://gov.wales/address-list-schools"]
    org_id_prefix = "GB-WALEDU"
    id_field = "School Number"
    date_fields = []
    source = {
        "title": "Address list of schools",
        "description": "",
        "identifier": "walesschools",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Welsh Government",
            "website": "https://gov.wales/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "",
                "title": "Address list of schools"
            }
        ],
    }

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback=self.find_file)]

    def find_file(self, response):
        link = response.css("div.document a::attr(href)").extract_first()
        self.logger.info(link)
        self.source["distribution"][0]["downloadURL"] = link
        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()
        return [scrapy.Request(response.urljoin(link), callback=self.parse)]

    def parse(self, response):
        yield Source(**self.source)
        wb = get_data(io.BytesIO(response.body))
        for sheet in ['Maintained', 'Independent', 'PRU']:
            headers = wb[sheet][0]
            data = wb[sheet][1:]
            for k, row in enumerate(data):

                if self.settings.getbool("DEBUG_ENABLED") and k >= self.settings.getint("DEBUG_ROWS", 100):
                    break

                row = dict(zip(headers, row))
                row["type"] = sheet
                yield self.parse_row(row)

    def parse_row(self, record):

        record = self.clean_fields(record)

        address4 = ", ".join([
            record.get("Address {}".format(f))
            for f in [3, 4]
            if record.get("Address {}".format(f))
        ])

        return Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("School Name"),
            "charityNumber": None,
            "companyNumber": None,
            "streetAddress": record.get("Address 1"),
            "addressLocality": record.get("Address 2"),
            "addressRegion": address4,
            "addressCountry": "Wales",
            "postalCode": self.parse_postcode(record.get("Postcode")),
            "telephone": record.get("Phone Number"),
            "alternateName": [],
            "email": None,
            "description": None,
            "organisationType": self.get_org_types(record),
            "url": None,
            "location": self.get_locations(record),
            "latestIncome": None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": None,
            "dateRemoved": None,
            "active": True,
            "parent": None,
            "orgIDs": [self.get_org_id(record)],
            "sources": [self.source["identifier"]],
        })

    def get_org_types(self, record):
        org_types = [
            "Education",
        ]
        for f in ["Sector", "Governance - see notes", "Welsh Medium Type - see notes", "School Type", "type"]:
            if record.get(f):
                if record.get(f) == "PRU":
                    org_types.append("Pupil Referral Unit")
                else:
                    org_types.append(record[f] + " School")
        return org_types

    def get_locations(self, record):
        locations = []
        if WAL_LAS.get(record.get("Local Authority")):
            code = WAL_LAS.get(record.get("Local Authority"))
            locations.append({
                "id": code,
                "name": record.get("Local Authority"),
                "geoCode": code,
                "geoCodeType": AREA_TYPES.get(code[0:3], "Local Authority"),
            })
        return locations
