# -*- coding: utf-8 -*-
import datetime
import re
import csv
import io

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation, AREA_TYPES

REGION_CONVERT = {
    "A": "E12000001",
    "B": "E12000002",
    "D": "E12000003",
    "E": "E12000004",
    "F": "E12000005",
    "G": "E12000006",
    "H": "E12000007",
    "J": "E12000008",
    "K": "E12000009",
}

class GIASSpider(BaseScraper):
    name = 'schools_gias'
    allowed_domains = ['service.gov.uk', 'ea-edubase-api-prod.azurewebsites.net']
    start_urls = ["https://get-information-schools.service.gov.uk/Downloads"]
    org_id_prefix = "GB-EDU"
    id_field = "URN"
    source = {
        "title": "Get information about schools",
        "description": "",
        "identifier": "gias",
        "license": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Department for Education",
            "website": "https://www.gov.uk/government/organisations/department-for-education",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "",
                "title": "Establishment fields CSV"
            }
        ],
    }

    date_format = "%d-%m-%Y"
    gias_regex = re.compile(r"http://ea-edubase-api-prod.azurewebsites.net/edubase/edubasealldata[0-9]{8}\.csv")
    date_fields = ["OpenDate", "CloseDate"]
    location_fields = ["GOR", "DistrictAdministrative", "AdministrativeWard",
                       "ParliamentaryConstituency", "UrbanRural", "MSOA", "LSOA"]

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback=self.find_csv)]

    def find_csv(self, response):
        link = response.css("a::attr(href)").re_first(self.gias_regex)
        self.logger.info(link)
        self.source["distribution"][0]["downloadURL"] = link
        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()
        return [scrapy.Request(response.urljoin(link), callback=self.parse_csv)]

    def parse_row(self, record):

        record = self.clean_fields(record)

        return Organisation(
            id=self.get_org_id(record),
            name=record.get("EstablishmentName"),
            charityNumber=None,
            companyNumber=None,
            streetAddress=record.get("Street"),
            addressLocality=record.get("Locality"),
            addressRegion=record.get("Address3"),
            addressCountry=record.get("Country (name)"),
            postalCode=record.get("Postcode"),
            telephone=record.get("TelephoneNum"),
            alternateName=[],
            email=None,
            description=None,
            organisationType=[
                "Education",
                record.get("EstablishmentTypeGroup (name)"),
                record.get("TypeOfEstablishment (name)"),
            ],
            url=record.get("SchoolWebsite"),
            location=self.get_locations(record),
            latestIncome=None,
            dateModified=datetime.datetime.now(),
            dateRegistered=record.get("OpenDate"),
            dateRemoved=record.get("CloseDate"),
            active=record.get("EstablishmentStatus (name)") != "Closed",
            parent=record.get("PropsName"),
            orgIDs=self.get_org_ids(record),
            sources=[
                self.source
            ]
        )

    def get_org_ids(self, record):
        org_ids = [self.get_org_id(record)]
        if record.get("UKPRN"):
            org_ids.append("GB-UKPRN-{}".format(record.get("UKPRN")))
        if record.get("EstablishmentNumber") and record.get("LA (code)"):
            org_ids.append("GB-LAESTAB-{}/{}".format(
                record.get("LA (code)").rjust(3, "0"),
                record.get("EstablishmentNumber").rjust(4, "0"),
            ))
        return org_ids

    def get_locations(self, record):
        locations = []
        for f in self.location_fields:
            code = record.get(f+" (code)", "")
            name = record.get(f+" (name)", "")

            if name == "" and code == "":
                continue

            if f == "GOR":
                code = REGION_CONVERT.get(code, code)

            if code == "":
                code = name

            locations.append({
                "id": code,
                "name": record.get(f+" (name)"),
                "geoCode": code,
                "geoCodeType": AREA_TYPES.get(code[0:3], f),
            })

        return locations
