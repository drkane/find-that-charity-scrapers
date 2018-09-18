# -*- coding: utf-8 -*-
import scrapy


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
    url              = scrapy.Field()
    location         = scrapy.Field()
    latestIncome     = scrapy.Field()
    dateModified     = scrapy.Field()
    dateRegistered   = scrapy.Field()
    dateRemoved      = scrapy.Field()
    active           = scrapy.Field()
    parent           = scrapy.Field()
    orgIDs           = scrapy.Field()
    sources          = scrapy.Field()

    # @TODO:
    # Add `latestIncomeDate` field to show when it was added
    # Add `CompleteName` field for autocomplete
    # Add `Classification` fields?

    def __repr__(self):
        return '<Org {} "{}"{}>'.format(
            self.get("id"), 
            self.get("name"),
            " INACTIVE" if not self.get("active") else ""
        )

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
