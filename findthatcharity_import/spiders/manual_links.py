# -*- coding: utf-8 -*-
import datetime
import io
import csv

import scrapy

from .base_scraper import BaseScraper
from ..items import Link, Source


# Look up manual links held in charity-lookups repository
class LinksSpider(BaseScraper):
    name = 'manual_links'
    allowed_domains = ['raw.githubusercontent.com']

    sources = [
        {
            "title": "University Charity Numbers",
            "description": "",
            "identifier": "university_charity_numbers",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/university-charity-number.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/university-charity-number.csv",
                    "title": "University Charity Numbers"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-HESA-{}".format(row["HESA ID"]),
                "organisation_id_b": row["OrgID"]
            }
        },
        {
            "title": "Dual Registered UK Charities",
            "description": "A list of charities registered in both England & Wales and Scotland",
            "identifier": "dual_registered",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/dual-registered-uk-charities.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/dual-registered-uk-charities.csv",
                    "title": "Dual Registered UK Charities"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-SC-{}".format(row["Scottish Charity Number"].strip()),
                "organisation_id_b": "GB-CHC-{}".format(row["E&W Charity Number"].strip()),
            }
        },
        {
            "title": "Registered housing providers",
            "description": "A list of charity numbers and company numbers found for registered housing providers",
            "identifier": "rsp_charity_company",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/rsp-charity-number.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/rsp-charity-number.csv",
                    "title": "Registered housing providers"
                }
            ],
            "_parse_csv": "rsp_charity_company_csv"
        },
        {
            "title": "University Royal Charters",
            "description": "A list of royal charter company numbers for universities",
            "identifier": "university_royal_charters",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/university-royal-charters.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/university-royal-charters.csv",
                    "title": "University Royal Charters"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-EDU-{}".format(row["URN"].strip()),
                "organisation_id_b": "GB-COH-{}".format(row["CompanyNumber"].strip()),
            }
        },
        {
            "title": "Independent Schools Charity Numbers",
            "description": "A list of charity numbers for independent schools",
            "identifier": "independent_schools_ew",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/independent-schools-ew.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/independent-schools-ew.csv",
                    "title": "Independent Schools Charity Numbers"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-EDU-{}".format(row["URN"].strip()),
                "organisation_id_b": "GB-CHC-{}".format(row["charity_number"].strip()) if row["charity_number"].strip() else None,
            }
        },
        {
            "title": "Register of Mergers",
            "description": "Register of Mergers kept by the Charity Commission for England and Wales.",
            "identifier": "rom",
            "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/",
            "license_name": "Open Government Licence v2.0",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "Charity Commission for England and Wales",
                "website": "https://www.gov.uk/charity-commission",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/ccew-register-of-mergers.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/ccew-register-of-mergers.csv",
                    "title": "Register of Mergers"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-CHC-{}".format(row["transferor_regno"].strip()) if row["transferor_subno"].strip()=="0" else None,
                "organisation_id_b": "GB-CHC-{}".format(row["transferee_regno"].strip()) if row["transferee_subno"].strip() == "0" else None,
                "description": "merger"
            }
        },
        {
            "title": "CIO Company Numbers",
            "description": "Match between CIO company numbers held by Companies House and their charity number",
            "identifier": "cio_company_numbers",
            "license": "",
            "license_name": "",
            "issued": "",
            "modified": "",
            "publisher": {
                "name": "David Kane",
                "website": "https://github.com/drkane/charity-lookups",
            },
            "distribution": [
                {
                    "downloadURL": "https://raw.githubusercontent.com/drkane/charity-lookups/master/cio_company_numbers.csv",
                    "accessURL": "https://github.com/drkane/charity-lookups/blob/master/cio_company_numbers.csv",
                    "title": "CIO Company Numbers"
                }
            ],
            "_parse_row": lambda row: {
                "organisation_id_a": "GB-COH-{}".format(row["company_number"].strip()) if row["company_number"].strip() else None,
                "organisation_id_b": "GB-CHC-{}".format(row["charity_number"].strip()) if row["charity_number"].strip() else None,
            }
        },
    ]

    def start_requests(self):

        for s in self.sources:
            yield scrapy.Request(
                s["distribution"][0]["downloadURL"],
                callback=getattr(self, s.get("_parse_csv", "parse_csv")),
                cb_kwargs=dict(source=s)
            )

    def parse_csv(self, response, source):

        yield Source(**{k: v for k, v in source.items() if not k.startswith("_")})

        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                if "_parse_row" in source:
                    row = source["_parse_row"](row)
                row["source"] = source["identifier"]
                if row.get("organisation_id_a") and row.get("organisation_id_b"):
                    yield Link(**row)

    def rsp_charity_company_csv(self, response, source):

        yield Source(**{k: v for k, v in source.items() if not k.startswith("_")})

        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                if row["Charity Number"].strip() != "":
                    yield Link(**{
                        "organisation_id_a": "GB-SHPE-{}".format(row["RP Code"].strip()),
                        "organisation_id_b": "GB-CHC-{}".format(row["Charity Number"].strip()),
                        "source": source["identifier"]
                    })
                if row["Company Number"].strip() != "":
                    yield Link(**{
                        "organisation_id_a": "GB-SHPE-{}".format(row["RP Code"].strip()),
                        "organisation_id_b": "GB-COH-{}".format(row["Company Number"].strip()),
                        "source": source["identifier"]
                    })
