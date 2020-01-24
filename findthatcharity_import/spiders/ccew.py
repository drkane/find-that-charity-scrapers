# -*- coding: utf-8 -*-
import datetime
import io
import csv
import zipfile
import re
import bcp
import tempfile
import os

import scrapy
import tqdm

from .base_scraper import BaseScraper
from ..items import Organisation, Source, AREA_TYPES

class CCEWSpider(BaseScraper):
    name = 'ccew'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 180 * 3
    }
    allowed_domains = ['charitycommission.gov.uk']
    start_urls = [
        "http://data.charitycommission.gov.uk/",
        "https://raw.githubusercontent.com/drkane/charity-lookups/master/cc-aoo-gss-iso.csv",
    ]
    org_id_prefix = "GB-CHC"
    id_field = "regno"
    date_fields = []
    date_format = "%Y-%m-%d %H:%M:%S"
    zip_regex = re.compile(r"http://apps.charitycommission.gov.uk/data/.*?/RegPlusExtract.*?\.zip")
    source = {
        "title": "Registered charities in England and Wales",
        "description": "Data download service provided by the Charity Commission",
        "identifier": "ccew",
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
                "downloadURL": "",
                "accessURL": "",
                "title": "Registered charities in England and Wales"
            }
        ],
    }
    ccew_files = {
        'extract_charity': [
            "regno", #	     integer 	    registered number of a charity
            "subno", #	     integer 	    subsidiary number of a charity (may be 0 for main/group charity)
            "name", #	     varchar(150) 	main name of the charity
            "orgtype", #	 varchar(2) 	R (registered) or RM (removed)
            "gd", #	         varchar(250) 	Description of Governing Document
            "aob", #	     varchar(175) 	area of benefit - may not be defined
            "aob_defined", # char(1) 	    area of benefit defined by Governing Document (T/F)
            "nhs", #         char(1) 	    NHS charity (T/F)
            "ha_no", #	     varchar(20) 	Housing Association number
            "corr", #	     varchar(70) 	Charity correspondent name
            "add1", #	     varchar(35) 	address line of charity's correspondent
            "add2", #	     varchar(35) 	address line of charity's correspondent
            "add3", #	     varchar(35) 	address line of charity's correspondent
            "add4", #	     varchar(35) 	address line of charity's correspondent
            "add5", #	     varchar(35) 	address line of charity's correspondent
            "postcode", #	 varchar(8) 	postcode of charity's correspondent
            "phone", #	     varchar(23) 	telephone of charity's correspondent
            "fax", #	     varchar(23) 	fax of charity's correspondent
        ],
        'extract_main_charity': [
            "regno", # 	    integer 	    registered number of a charity
            "coyno", # 	    integer 	    company registration number
            "trustees", # 	char(1) 	    trustees incorporated (T/F)
            "fyend", # 	    char(4) 	    Financial year end
            "welsh", # 	    char(1) 	    requires correspondence in both Welsh & English (T/F)
            "incomedate", # datetime 	    date for latest gross income (blank if income is an estimate)
            "income", # 	integer
            "grouptype", # 	varchar(4) 	    may be blank
            "email", # 	    varchar(255) 	email address
            "web", # 	    varchar(255) 	website address
        ],
        'extract_name': [
            "regno",  # 	integer 	    registered number of a charity
            "subno",  # 	integer 	    subsidiary number of a charity (may be 0 for main/group charity)
            "nameno", #  	integer 	    number identifying a charity name
            "name",   #     varchar(150) 	name of a charity (multiple occurrences possible)
        ],
        'extract_registration': [
            "regno", #   	integer 	    registered number of a charity
            "subno", #   	integer 	    subsidiary number of a charity (may be 0 for main/group charity)
            "regdate", #    datetime 	    date of registration for a charity
            "remdate", #    datetime 	    Removal date of a charity - Blank for Registered Charities
            "remcode", #    varchar(3) 	    Register removal reason code
        ],
        'extract_charity_aoo': [
            "regno", # 	    integer 	    registered number of a charity
            "aootype", # 	char(1) 	    A B or D
            "aookey", # 	integer 	    up to three digits
            "welsh", # 	    char(1) 	    Flag: Y or blank
            "master", # 	integer 	    may be blank. If aootype=D then holds continent; if aootype=B then holds GLA/met county
        ],
        # 'extract_objects': [
        #     "regno",  # 	integer 	    egistered number of a charity
        #     "subno",  # 	integer 	    ubsidiary number of a charity (may be 0 for main/group charity)
        #     "seqno",  # 	char(4) 	    equence number (in practice 0-20)
        #     "object", #  	varchar(255) 	Description of objects of a charity
        # ],
    }

    def start_requests(self):
        return [
            scrapy.Request(self.start_urls[1], callback=self.download_aoo_ref)
        ]

    def download_aoo_ref(self, response):

        self.aooref = {}
        with io.StringIO(response.text) as a:
            csvreader = csv.DictReader(a)
            for row in csvreader:
                self.aooref[(row["aootype"], row["aookey"])] = row

        self.logger.info("Imported AOO reference data")
        return scrapy.Request(self.start_urls[0], callback=self.fetch_zip)

    def fetch_zip(self, response):
        link = response.css("a::attr(href)").re_first(self.zip_regex)
        self.source["distribution"][0]["downloadURL"] = link
        self.source["distribution"][0]["accessURL"] = self.start_urls[0]
        self.source["modified"] = datetime.datetime.now().isoformat()
        return scrapy.Request(response.urljoin(link), callback=self.process_zip)

    def process_zip(self, response):
        self.logger.info("File size: {}".format(len(response.body)))
        self.charities = {}
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            cczip_name = os.path.join(tmpdirname, 'ccew.zip')
            files = {}

            with open(cczip_name, 'wb') as cczip:
                self.logger.info("Saving ZIP to disk")
                cczip.write(response.body)

            with zipfile.ZipFile(cczip_name, 'r') as z:
                for f in z.infolist():
                    filename = f.filename.replace(".bcp", "")
                    filepath = os.path.join(tmpdirname, f.filename)
                    if filename not in self.ccew_files.keys():
                        self.logger.debug("Skipping: {}".format(f.filename))
                        continue
                    self.logger.info("Saving {} to disk".format(f.filename))
                    z.extract(f, path=tmpdirname)
                    files[filename] = filepath

            for filename, filepath in files.items():
                with open(filepath, 'r', encoding='latin1') as bcpfile:
                    self.logger.info("Processing: {}".format(filename))
                    self.process_bcp(bcpfile, filename)

        return self.process_charities()

    def process_bcp(self, bcpfile, filename):

        fields = self.ccew_files.get(filename)
        self.date_fields = [f for f in fields if f.endswith("date")]

        bcpreader = bcp.DictReader(bcpfile, fieldnames=fields)
        for k, row in tqdm.tqdm(enumerate(bcpreader)):
            if self.settings.getbool("DEBUG_ENABLED") and k > 100: #self.settings.getint("DEBUG_ROWS", 100):
                break
            row = self.clean_fields(row)
            if not row.get("regno"):
                continue
            if row["regno"] not in self.charities:
                self.charities[row["regno"]] = {
                    f: [] for f in self.ccew_files.keys()
                }
            if (filename in ["extract_main_charity", "extract_charity"] and row.get("subno", '0') == '0'):
                for field in row:
                    self.charities[row["regno"]][field] = row[field]
            else:
                self.charities[row["regno"]][filename].append(row)

    def process_charities(self):
        yield Source(**self.source)
        
        for regno, record in self.charities.items():

            # helps with debugging - shouldn't normally be empty
            record["regno"] = regno

            # work out registration dates
            registration_date, removal_date = self.get_regdates(record)

            # work out org_types and org_ids
            org_types = [
                "Registered Charity",
                "Registered Charity (England and Wales)"
            ]
            org_ids = [self.get_org_id(record)]
            coyno = self.parse_company_number(record.get("coyno"))
            if coyno:
                org_types.append("Registered Company")
                org_types.append("Incorporated Charity")
                org_ids.append("GB-COH-{}".format(coyno))

            # check for CIOs
            if record.get("gd") and record["gd"].startswith("CIO - "):
                org_types.append("Charitable Incorporated Organisation")
                if record["gd"].lower().startswith("cio - association"):
                    org_types.append("Charitable Incorporated Organisation - Association")
                elif record["gd"].lower().startswith("cio - foundation"):
                    org_types.append("Charitable Incorporated Organisation - Foundation")

            yield Organisation(**{
                "id": self.get_org_id(record),
                "name": self.parse_name(record.get("name")),
                "charityNumber": record.get("regno"),
                "companyNumber": coyno,
                "streetAddress": record.get("add1"),
                "addressLocality": record.get("add2"),
                "addressRegion": record.get("add3"),
                "addressCountry": record.get("add4"),
                "postalCode": self.parse_postcode(record.get("postcode")),
                "telephone": record.get("phone"),
                "alternateName": [
                    self.parse_name(c["name"])
                    for c in record.get("extract_name", [])
                ],
                "email": record.get("email"),
                "description": self.get_objects(record),
                "organisationType": org_types,
                "url": self.parse_url(record.get("web")),
                "location": self.get_locations(record),
                "latestIncome": int(record["income"]) if record.get("income") else None,
                "dateModified": datetime.datetime.now(),
                "dateRegistered": registration_date,
                "dateRemoved": removal_date,
                "active": record.get("orgtype") == "R",
                "parent": None,
                "orgIDs": org_ids,
                "source": self.source["identifier"],
            })

    def get_locations(self, record):
        # work out locations
        locations = []
        for l in record.get("extract_charity_aoo", []):
            aookey = (l["aootype"], l["aookey"])
            aoo = self.aooref.get(aookey)
            if not aoo:
                continue
            if aoo.get("GSS", "") != "":
                locations.append({
                    "id": aoo["GSS"],
                    "name": aoo["aooname"],
                    "geoCode": aoo["GSS"],
                    "geoCodeType": AREA_TYPES.get(aoo["GSS"][0:3], "Unknown"),
                })
            elif aoo.get("ISO3166-1", "") != "":
                locations.append({
                    "id": aoo["ISO3166-1"],
                    "name": aoo["aooname"],
                    "geoCode": aoo["ISO3166-1"],
                    "geoCodeType": "ISO3166-1",
                })
        return locations

    def get_regdates(self, record):
        reg = [r for r in record.get("extract_registration", []) if r.get("regdate") and (r.get("subno") == '0')]
        reg = sorted(reg, key=lambda x: x.get("regdate"))
        if not reg:
            return (None, None)

        return (
            reg[0].get("regdate"),
            reg[-1].get("remdate")
        )

    def get_objects(self, record):
        objects = []
        for o in record.get("extract_objects", []):
            if o.get("subno") == '0' and isinstance(o['object'], str):
                objects.append(re.sub("[0-9]{4}$", "", o['object']))
        return ''.join(objects)
