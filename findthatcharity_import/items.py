# -*- coding: utf-8 -*-
import math

import dateutil.parser
import scrapy

from .db import tables

class Organisation(scrapy.Item):
    """
    Item representing an organisation from the scrapers
    """
    id               = scrapy.Field()
    name             = scrapy.Field()
    charityNumber    = scrapy.Field()
    companyNumber    = scrapy.Field()
    streetAddress    = scrapy.Field()
    addressLocality  = scrapy.Field()
    addressRegion    = scrapy.Field()
    addressCountry   = scrapy.Field()
    postalCode       = scrapy.Field()
    telephone        = scrapy.Field()
    alternateName    = scrapy.Field()
    email            = scrapy.Field()
    description      = scrapy.Field()
    organisationType = scrapy.Field()
    organisationTypePrimary = scrapy.Field()
    url              = scrapy.Field()
    location         = scrapy.Field()
    latestIncome     = scrapy.Field()
    latestIncomeDate  = scrapy.Field()
    dateModified     = scrapy.Field()
    dateRegistered   = scrapy.Field()
    dateRemoved      = scrapy.Field()
    active           = scrapy.Field()
    parent           = scrapy.Field()
    orgIDs           = scrapy.Field()
    source           = scrapy.Field()

    # @TODO:
    # Add `CompleteName` field for autocomplete
    # Add `Classification` fields?

    def __repr__(self):
        return '<Org {} "{}"{}>'.format(
            self.get("id"),
            self.get("name"),
            " INACTIVE" if not self.get("active") else ""
        )

    def to_elasticsearch(self):
        es_item = dict(self)
        es_item["_index"] = self.__class__.__name__.lower()
        es_item["_op_type"] = "index"
        es_item["_id"] = es_item["id"]
        del es_item["id"]

        # get names
        es_item["complete_names"] = {
            "input": self.get_complete_names(es_item),
            "weight": max(1, math.ceil(math.log1p((es_item.get("latestIncome", 0) or 0))))
        }

        return es_item

    @staticmethod
    def es_mapping():
        return {
            "properties": {
                "geo": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                },
                "complete_names": {
                    "type": "completion",
                    "contexts": [
                        {
                            "name": "organisationType",
                            "type": "category",
                            "path": "organisationType"
                        }
                    ],
                }
            }
        }

    def to_mongodb(self):
        md_item = dict(self)
        md_item["_id"] = md_item["id"]
        del md_item["id"]
        return ('organisation', md_item)

    def get_complete_names(self, item):

        # get names
        all_names = []
        
        # add main name
        if item.get("name"):
            all_names.append(item.get("name"))

        # add alternate names
        if isinstance(item.get("alternateName"), list):
            all_names.extend(item.get("alternateName"))
        elif isinstance(item.get("alternateName"), str):
            all_names.append(item.get("alternateName"))

        words = set()
        for n in all_names:
            if n:
                w = n.split()
                words.update([" ".join(w[r:]) for r in range(len(w))])
        return list(words)

    def to_tables(self):
        return {
            "organisation": [{
                c.name: self.get(c.name, None) for c in tables["organisation"].columns
            }],
            "organisation_links": [{
                "organisation_id_a": self.get("id"),
                "organisation_id_b": i,
                "source": self.get("source")
            } for i in self.get("orgIDs", []) if i and i != self.get("id")],
        }



class Source(scrapy.Item):
    """
    Item representing a data source from the scrapers
    """
    title = scrapy.Field()
    description = scrapy.Field()
    identifier = scrapy.Field()
    license = scrapy.Field()
    license_name = scrapy.Field()
    issued = scrapy.Field()
    modified = scrapy.Field()
    publisher = scrapy.Field()
    distribution = scrapy.Field()

    def __repr__(self):
        return '<Source "{}">'.format(self.get("title"))

    def to_elasticsearch(self):
        es_item = dict(self)
        es_item["_index"] = self.__class__.__name__.lower()
        es_item["_op_type"] = "index"
        es_item["_id"] = es_item["identifier"]
        return es_item

    def to_mongodb(self):
        md_item = dict(self)
        md_item["_id"] = md_item["identifier"]
        return ('source', md_item)

    def to_tables(self):
        source = {}
        for c in tables["source"].columns:
            source[c.name] = self.get(c.name, None)
            if source[c.name] == "":
                source[c.name] = None
        if source["modified"] and isinstance(source["modified"], str):
            source["modified"] = dateutil.parser.parse(source["modified"])
        if source["issued"] and isinstance(source["issued"], str):
            source["issued"] = dateutil.parser.parse(source["issued"])
        source["publisher_name"] = self.get("publisher", {}).get("name")
        source["publisher_website"] = self.get("publisher", {}).get("website")

        return {
            "source": [source],
        }

class Link(scrapy.Item):
    """
    Item representing a data source from the scrapers
    """
    organisation_id_a = scrapy.Field()
    organisation_id_b = scrapy.Field()
    description = scrapy.Field()
    source = scrapy.Field()

    def __repr__(self):
        return '<Link {} and {}>'.format(self.get("organisation_id_a"), self.get("organisation_id_b"))

    def to_tables(self):

        return {
            "organisation_links": [{
                c.name: self.get(c.name, None) for c in tables["organisation_links"].columns
            }],
        }

class Identifier(scrapy.Item):
    """
    Item representing a entry in the org-id list of identifiers
    """
    code = scrapy.Field()
    description_en = scrapy.Field()
    License = scrapy.Field()
    access_availableOnline = scrapy.Field()
    access_exampleIdentifiers = scrapy.Field()
    access_guidanceOnLocatingIds = scrapy.Field()
    access_languages = scrapy.Field()
    access_onlineAccessDetails = scrapy.Field()
    access_publicDatabase = scrapy.Field()
    confirmed = scrapy.Field()
    coverage = scrapy.Field()
    data_availability = scrapy.Field()
    data_dataAccessDetails = scrapy.Field()
    data_features = scrapy.Field()
    data_licenseDetails = scrapy.Field()
    data_licenseStatus = scrapy.Field()
    deprecated = scrapy.Field()
    formerPrefixes = scrapy.Field()
    links_opencorporates = scrapy.Field()
    links_wikipedia = scrapy.Field()
    listType = scrapy.Field()
    meta_lastUpdated = scrapy.Field()
    meta_source = scrapy.Field()
    name_en = scrapy.Field()
    name_local = scrapy.Field()
    quality = scrapy.Field()
    quality_explained_Availability_API = scrapy.Field()
    quality_explained_Availability_BulkDownload = scrapy.Field()
    quality_explained_Availability_CSVFormat = scrapy.Field()
    quality_explained_Availability_ExcelFormat = scrapy.Field()
    quality_explained_Availability_JSONFormat = scrapy.Field()
    quality_explained_Availability_PDFFormat = scrapy.Field()
    quality_explained_Availability_RDFFormat = scrapy.Field()
    quality_explained_Availability_XMLFormat = scrapy.Field()
    quality_explained_License_ClosedLicense = scrapy.Field()
    quality_explained_License_NoLicense = scrapy.Field()
    quality_explained_License_OpenLicense = scrapy.Field()
    quality_explained_ListType_Local = scrapy.Field()
    quality_explained_ListType_Primary = scrapy.Field()
    quality_explained_ListType_Secondary = scrapy.Field()
    quality_explained_ListType_ThirdParty = scrapy.Field()
    registerType = scrapy.Field()
    sector = scrapy.Field()
    structure = scrapy.Field()
    subnationalCoverage = scrapy.Field()
    url = scrapy.Field()

    def __repr__(self):
        return '<Identifier {}>'.format(self.get("code"))

    def to_tables(self):

        return {
            "identifier": [{
                c.name: self.get(c.name, None) for c in tables["identifier"].columns
            }],
        }


AREA_TYPES = {
    "E00": "OA",
    "E01": "LSOA",
    "E02": "MSOA",
    "E04": "PAR",
    "E05": "WD",
    "E06": "UA",
    "E07": "NMD",
    "E08": "MD",
    "E09": "LONB",
    "E10": "CTY",
    "E12": "RGN/GOR",
    "E14": "WPC",
    "E15": "EER",
    "E21": "CANNET",
    "E22": "CSP",
    "E23": "PFA",
    "E25": "PUA",
    "E26": "NPARK",
    "E28": "REGD",
    "E29": "REGSD",
    "E30": "TTWA",
    "E31": "FRA",
    "E32": "LAC",
    "E33": "WZ",
    "E36": "CMWD",
    "E37": "LEP",
    "E38": "CCG",
    "E39": "NHSAT",
    "E41": "CMLAD",
    "E42": "CMCTY",
    "N00": "SA",
    "N06": "WPC",
    "S00": "OA",
    "S01": "DZ",
    "S02": "IG",
    "S03": "CHP",
    "S05": "ROA - CPP",
    "S06": "ROA - Local",
    "S08": "HB",
    "S12": "CA",
    "S13": "WD",
    "S14": "WPC",
    "S16": "SPC",
    "S22": "TTWA",
    "W00": "OA",
    "W01": "LSOA",
    "W02": "MSOA",
    "W03": "USOA",
    "W04": "COM",
    "W05": "WD",
    "W06": "UA",
    "W07": "WPC",
    "W09": "NAWC",
    "W14": "CDRP",
    "W20": "REGD",
    "W21": "REGSD",
    "W22": "TTWA",
    "W30": "AgricSmall",
    "W33": "CFA",
    "W35": "WZ",
    "W39": "CMWD",
    "W40": "CMLAD",
    "W41": "CMCTY",
}
