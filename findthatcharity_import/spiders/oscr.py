# -*- coding: utf-8 -*-
import datetime
import io
import csv
import zipfile

import scrapy

from .base_scraper import BaseScraper
from ..items import Organisation

class OSCRSpider(BaseScraper):
    name = 'oscr'
    allowed_domains = ['oscr.org.uk', 'githubusercontent.com']
    start_urls = [
        "https://www.oscr.org.uk/about-charities/search-the-register/charity-register-download",
        "https://raw.githubusercontent.com/drkane/charity-lookups/master/dual-registered-uk-charities.csv",
    ]
    org_id_prefix = "GB-SC"
    id_field = "Charity Number"
    date_fields = ["Registered Date", "Year End"]
    date_format = {
        "Registered Date": "%d/%m/%Y %H:%M",
        "Year End": "%d/%m/%Y"
    }
    source = {
        "title": "Office of Scottish Charity Regulator Charity Register Download",
        "description": "",
        "identifier": "oscr",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/",
        "license_name": "Open Government Licence v2.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Office of Scottish Charity Regulator",
            "website": "https://www.oscr.org.uk/",
        },
        "distribution": [
            {
                "downloadURL": "",
                "accessURL": "",
                "title": "Office of Scottish Charity Regulator Charity Register Download"
            }
        ],
    }

    def start_requests(self):
        return [scrapy.Request(self.start_urls[1], callback=self.download_dual)]

    def download_dual(self, response):

        self.dual_registered = {}
        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                regno = row["Scottish Charity Number"].strip()
                if regno not in self.dual_registered:
                    self.dual_registered[regno] = []
                self.dual_registered[regno].append(row["E&W Charity Number"].strip())

        return [scrapy.Request(self.start_urls[0], callback=self.fetch_zip)]

    def fetch_zip(self, response, agreeterms=True):
        TERMS_AND_CONDITIONS_TEXT = "ContentPlaceHolderDefault_WebsiteContent_ctl05_CharityRegDownload_10_lblTermsConditions"
        self.logger.info("Using url: %s" % response.url)

        formdata = {
            "ctl00$ctl00$ctl00$ContentPlaceHolderDefault$WebsiteContent$ctl05$CharityRegDownload_10$cbTermsConditions": "on",
            "ctl00$ctl00$ctl00$ContentPlaceHolderDefault$WebsiteContent$ctl05$CharityRegDownload_10$uxTemp": "",
            "ctl00$ctl00$ctl00$ContentPlaceHolderDefault$WebsiteContent$ctl05$CharityRegDownload_10$btnProceed": "Proceed"
        }
        for i in ["__EVENTTARGET", "__EVENTARGUMENT", "__VIEWSTATE", "__VIEWSTATEGENERATOR", "_TSM_HiddenField_"]:
            formdata[i] = response.css('[name="{}"]::attr(value)'.format(i)).extract_first()
            if not formdata[i]:
                formdata[i] = ""

        # get the terms and conditions box
        tandcs = "".join(["\r\n" if t=="" else t.strip() for t in response.css("#{} *::text".format(TERMS_AND_CONDITIONS_TEXT)).extract()])
        if not agreeterms:
            self.logger.info("To continue accept the following terms and conditions")
            self.logger.info(tandcs)
            accept = input(
                "Do you accept the terms and conditions? (y/n) ")
            if accept[0].strip().lower() != "y":
                self.logger.error(
                    "Did not download OSCR data as terms and conditions not accepted")
                return
        else:
            self.logger.info(tandcs)

        self.logger.debug(formdata)

        self.source["distribution"][0]["downloadURL"] = self.start_urls[0]
        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()

        return scrapy.FormRequest(url=response.url, 
                                   method='POST', 
                                   formdata=formdata, 
                                   callback=self.process_zip)


    def process_zip(self, response):
        csvs = []
        self.logger.info("File size: {}".format(len(response.body)))
        with zipfile.ZipFile(io.BytesIO(response.body)) as z:
            for f in z.infolist():
                self.logger.info("Opening: {}".format(f.filename))
                with z.open(f) as csvfile:
                    csvs.append(response.replace(
                        body=csvfile.read()))
        return self.parse_csv(csvs[0])

    def parse_row(self, record):

        record = self.clean_fields(record)

        address, _ = self.split_address(record.get("Principal Office/Trustees Address", ""), get_postcode=False)

        org_types = [
            "Registered Charity",
            "Registered Charity (Scotland)",
        ]
        if record.get("Regulatory Type") != "Standard":
            org_types.append(record.get("Regulatory Type"))
        if record.get("Designated religious body") == "Yes":
            org_types.append("Designated religious body")
        if record.get("Constitutional Form") != "Other":
            org_types.append(record.get("Constitutional Form"))

        org_ids = [self.get_org_id(record)]
        for i in self.dual_registered.get(record.get(self.id_field), []):
            org_ids.append("GB-CHC-{}".format(i))

        return Organisation(**{
            "id": self.get_org_id(record),
            "name": record.get("Charity Name"),
            "charityNumber": record.get(self.id_field),
            "companyNumber": None,
            "streetAddress": address[0],
            "addressLocality": address[1],
            "addressRegion": address[2],
            "addressCountry": "Scotland",
            "postalCode": self.parse_postcode(record.get("Postcode")),
            "telephone": None,
            "alternateName": [record.get("Known As")] if record.get("Known As") else [],
            "email": None,
            "description": record.get("Objectives"),
            "organisationType": org_types,
            "url": self.parse_url(record.get("Website")),
            "location": [],
            "latestIncome": int(record["Most recent year income"]) if record.get("Most recent year income") else None,
            "dateModified": datetime.datetime.now(),
            "dateRegistered": record.get("Registered Date"),
            "dateRemoved": None,
            "active": record.get("Charity Status") != "Removed",
            "parent": record.get("Parent Charity Name"), # @TODO: More sophisticated getting of parent charities here
            "orgIDs": org_ids,
            "sources": [self.source],
        })
