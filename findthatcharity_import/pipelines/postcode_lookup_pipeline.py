# -*- coding: utf-8 -*-
import logging
import json
import csv
import io

import scrapy

from ..items import Organisation, AREA_TYPES

FIELDS_TO_COLLECT = [
    'cty', 'laua', 'ward', 'ctry', 'rgn', 'gor', 'pcon', 'ttwa', 'lsoa11', 'msoa11'
]

class PostcodeLookupPipeline(object):

    def __init__(self, pc_url, pc_field, pc_fields_to_add, stats):
        self.pc_url = pc_url
        self.pc_field = pc_field
        self.pc_fields_to_add = pc_fields_to_add
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            pc_url=crawler.settings.get('PC_URL', 'https://postcodes.findthatcharity.uk/postcodes/{}.json'),
            pc_field=crawler.settings.get('PC_FIELD', 'postalCode'),
            pc_fields_to_add=crawler.settings.get('PC_FIELDS_TO_ADD', FIELDS_TO_COLLECT),
            stats=crawler.stats,
        )

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):

        # only lookup organisations
        if not isinstance(item, Organisation):
            return item

        postcode = spider.parse_postcode(item.get(self.pc_field))

        # no useful postcodes
        if not postcode:
            self.stats.inc_value('postcode/postcode_missing', 1)
            return item

        if item['location']:
            self.stats.inc_value('postcode/geodata_exists', 1)
            return item

        request = scrapy.Request(self.pc_url.format(postcode))
        dfd = spider.crawler.engine.download(request, spider)
        dfd.addBoth(self.return_item, item)
        return dfd

    def return_item(self, response, item):
        if response.status != 200:
            # Error happened, return item.
            self.stats.inc_value('postcode/postcode_not_found', 1)
            return item

        postcode_data = json.loads(response.body_as_unicode())

        item['location'] = []

        postcode_attributes = postcode_data.get("data",{}).get("attributes", {})

        for geotype, geocode in postcode_attributes.items():
            name_field = '{}_name'.format(geotype)
            if geocode and \
                isinstance(geocode, str) and \
                name_field in postcode_attributes and \
                geotype in self.pc_fields_to_add and \
                not geocode.endswith('999999'):
                item['location'].append({
                    "id": geocode,
                    "name": postcode_attributes[name_field],
                    "countryCode": "GB",
                    "geoCode": geocode,
                    "geoCodeType": AREA_TYPES.get(geocode[0:3], geotype),
                    "description": "Based on postcode of registered office"
                })
                self.stats.inc_value('postcode/fields/{}_added'.format(geotype), 1)

        if postcode_attributes.get("lat", 0) != 0 and postcode_attributes.get("long", 0) != 0:
            item['location'].append({
                "id": "postcode-lat-long",
                "name": "Registered office (latitude and longitude based on the postcode)",
                "countryCode": "GB",
                "latitude": postcode_attributes["lat"],
                "longitude": postcode_attributes["long"],
                "description": "Based on postcode of registered office"
            })
            self.stats.inc_value('postcode/fields/latlong_added', 1)

        if item['location']:
            self.stats.inc_value('postcode/geodata_added', 1)
        else:
            self.stats.inc_value('postcode/geodata_not_added', 1)

        return item

        
