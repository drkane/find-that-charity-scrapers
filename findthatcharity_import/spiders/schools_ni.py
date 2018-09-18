# -*- coding: utf-8 -*-
import datetime

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation

class SchoolsNISpider(BaseScraper):
    name = 'schoolsni'
    allowed_domains = ['education-ni.gov.uk']
    start_urls = ["http://apps.education-ni.gov.uk/appinstitutes/default.aspx"]
    skip_rows = 5
    org_id_prefix = "GB-NIEDU"
    id_field = "Institution Reference Number"
    date_fields = ["Date Closed"]
    source = {
        "title": "Department of Education - Institution Search",
        "description": "",
        "identifier": "nideptofeducation",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Northern Ireland Department of Education",
            "website": "https://www.education-ni.gov.uk/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "",
                "title": "Institution Search"
            }
        ],
    }

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback=self.download_file)]

    def download_file(self, response):
        post_params = {
            "__EVENTARGUMENT": "",
            "__EVENTTARGET": "",
            "__EVENTVALIDATION": "",
            "__VIEWSTATE": "",
            "__VIEWSTATEENCRYPTED": "",
            "__VIEWSTATEGENERATOR": "",
            "as_fid": "",
            "as_sfid": "",
            "ctl00$ContentPlaceHolder1$instAddr$instAddr_hfv": "",
            "ctl00$ContentPlaceHolder1$instCounty": "",
            "ctl00$ContentPlaceHolder1$instMgt": "",
            "ctl00$ContentPlaceHolder1$instName$instName_hfv": "",
            "ctl00$ContentPlaceHolder1$instPhone$instPhone_hfv": "",
            "ctl00$ContentPlaceHolder1$instPostcode$instPostcode_hfv": "",
            "ctl00$ContentPlaceHolder1$instRef$instRef_hfv": "",
            "ctl00$ContentPlaceHolder1$instStatus": "-1",
            "ctl00$ContentPlaceHolder1$instTown": "",
            "ctl00$ContentPlaceHolder1$instType": "-1",
            "ctl00$ContentPlaceHolder1$lvSchools$exportFilename$exportFilename_hfv": "U2Nob29sc19QbHVzXzE3X1NlcF8yMDE4XzExXzQ3",
            "ctl00$ContentPlaceHolder1$lvSchools$exportType": "2",
        }
        for p in post_params:
            try:
                param = response.css("input[name={}]::attr(value)".format(p)).extract_first()
                post_params[p] = param if param else ''
            except:
                pass
        post_params["ctl00$ContentPlaceHolder1$instType"] = "-2"
        post_params["ctl00$ContentPlaceHolder1$lvSchools$exportType"] = "2"
        post_params["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$lvSchools$btnDoExport"
        post_params["__EVENTARGUMENT"] = ""
        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()
        return [scrapy.FormRequest(
            self.start_urls[0],
            method="POST",
            formdata=post_params,
            callback=self.parse_csv
        )]

    def parse_row(self, record):

        if record.get(self.id_field) == "UNKNOWN":
            return

        record = self.clean_fields(record)

        address = ", ".join([
            record.get("Address Line {}".format(f))
            for f in [1, 2, 3]
            if record.get("Address Line {}".format(f))
        ])

        return Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("Institution Name"),
            "charityNumber": None,
            "companyNumber": None,
            "streetAddress": address,
            "addressLocality": record.get("Town"),
            "addressRegion": record.get("Count"),
            "addressCountry": "Northern Ireland",
            "postalCode": self.parse_postcode(record.get("Postcode")),
            "telephone": record.get("Telephone"),
            "alternateName": [],
            "email": record.get("Email"),
            "description": None,
            "organisationType": [
                "Education",
                record.get("Management"),
                record.get("Type", "") + " School",
            ],
            "url": None,
            "location": [],
            "latestIncome": None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": None,
            "dateRemoved": record.get("Date Closed"),
            "active": record.get("Status") == "Open",
            "parent": None,
            "orgIDs": [self.get_org_id(record)],
            "sources": [self.source],
        })