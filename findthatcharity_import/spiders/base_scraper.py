import io
import csv
import datetime
import re

import scrapy
import validators
import titlecase

from ..items import Source

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

class BaseScraper(scrapy.Spider):

    date_format = DEFAULT_DATE_FORMAT
    date_fields = []
    encoding = "utf8"

    def parse_csv(self, response):

        try:
            csv_text = response.text
        except AttributeError:
            csv_text = response.body.decode(self.encoding)

        with io.StringIO(csv_text) as a:
            csvreader = csv.DictReader(a)
            for k, row in enumerate(csvreader):
                if self.settings.getbool("DEBUG_ENABLED") and k > self.settings.getint("DEBUG_ROWS", 100):
                    break
                yield self.parse_row(row)

        if hasattr(self, "source"):
            yield Source(**self.source)

    def get_org_id(self, record):
        return "-".join([self.org_id_prefix, str(record.get(self.id_field))])

    def clean_fields(self, record):
        for f in record.keys():
            # clean blank values
            if record[f] == "":
                record[f] = None

            # clean date fields
            elif f in self.date_fields:
                date_format = self.date_format
                if isinstance(date_format, dict):
                    date_format = date_format.get(f, DEFAULT_DATE_FORMAT)

                try:
                    if record.get(f):
                        record[f] = datetime.datetime.strptime(record.get(f).strip(), date_format)
                except ValueError:
                    record[f] = None

            # strip string fields
            elif isinstance(record[f], str):
                record[f] = record[f].strip()
        return record

    def slugify(self, value):
        value = value.lower()
        value = re.sub(r'\([0-9]+\)', "_", value).strip("_") # replace values in brackets
        value = re.sub(r'[^0-9A-Za-z]+', "_", value).strip("_") # replace any non-alphanumeric characters
        return value

    def parse_company_number(self, coyno):
        if not coyno:
            return None

        coyno = coyno.strip()
        if coyno == "":
            return None

        if coyno.isdigit():
            return coyno.rjust(8, "0")

        return coyno

    def split_address(self, address_str, address_parts=3, separator=", ", get_postcode=True):
        """
        Split an address string into postcode and address parts

        Will produce an array of exactly `address_parts` length, with None
        used in values that aren't present
        """
        if address_str is None:
            return ([None for n in range(address_parts)], None)

        address = [a.strip() for a in address_str.split(separator.strip())]
        postcode = None

        # if our list is greater than one item long then
        # we assume the last item is a postcode
        if get_postcode:
            if len(address) > 1:
                postcode = self.parse_postcode(address[-1])
                address = address[0:-1]
            else:
                return address, None

        # make a new address list that's exactly the right length
        new_address = [None for n in range(address_parts)]
        for k, _ in enumerate(new_address):
            if len(address) > k:
                if k+1 == address_parts:
                    new_address[k] = separator.join(address[k:])
                else:
                    new_address[k] = address[k]

        return new_address, postcode

    def parse_url(self, url):
        if url is None:
            return None

        url = url.strip()

        if validators.url(url):
            return url

        if validators.url("http://%s" % url):
            return "http://%s" % url

        if url in ["n.a", 'non.e', '.0', '-.-', '.none', '.nil', 'N/A', 'TBC',
                   'under construction', '.n/a', '0.0', '.P', b'', 'no.website']:
            return None

        for i in ['http;//', 'http//', 'http.//', 'http:\\\\',
                  'http://http://', 'www://', 'www.http://']:
            url = url.replace(i, 'http://')
        url = url.replace('http:/www', 'http://www')

        for i in ['www,', ':www', 'www:', 'www/', 'www\\\\', '.www']:
            url = url.replace(i, 'www.')

        url = url.replace(',', '.')
        url = url.replace('..', '.')

        if validators.url(url):
            return url

        if validators.url("http://%s" % url):
            return "http://%s" % url

    def parse_postcode(self, postcode):
        """
        standardises a postcode into the correct format
        """

        if postcode is None:
            return None

        # check for blank/empty
        # put in all caps
        postcode = postcode.strip().upper()
        if postcode == '':
            return None

        # replace any non alphanumeric characters
        postcode = re.sub('[^0-9a-zA-Z]+', '', postcode)

        if postcode == '':
            return None

        # check for nonstandard codes
        if len(postcode) > 7:
            return postcode

        first_part = postcode[:-3].strip()
        last_part = postcode[-3:].strip()

        # check for incorrect characters
        first_part = list(first_part)
        last_part = list(last_part)
        if last_part and last_part[0] == "O":
            last_part[0] = "0"

        return "%s %s" % ("".join(first_part), "".join(last_part))

    def title_exceptions(self, word, **kwargs):

        word_test = word.strip("(){}<>.")

        # lowercase words
        if word_test.lower() in ['a', 'an', 'of', 'the', 'is', 'or']:
            return word.lower()

        # uppercase words
        if word_test.upper() in ['UK', 'FM', 'YMCA', 'PTA', 'PTFA',
                                 'NHS', 'CIO', 'U3A', 'RAF', 'PFA', 'ADHD',
                                 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI',
                                 'AFC', 'CE', 'CIC'
                                ]:
            return word.upper()

        # words with no vowels that aren't all uppercase
        if word_test.lower() in ['st', 'mr', 'mrs', 'ms', 'ltd', 'dr', 'cwm', 'clwb', 'drs']:
            return None

        # words with number ordinals
        ord_numbers_re = re.compile("([0-9]+(?:st|nd|rd|th))")
        if bool(ord_numbers_re.search(word_test.lower())):
            return word.lower()

        # words with dots/etc in the middle
        for s in [".", "'", ")"]:
            dots = word.split(s)
            if len(dots) > 1:
                # check for possesive apostrophes
                if s == "'" and dots[-1].upper() == "S":
                    return s.join([titlecase.titlecase(i, self.title_exceptions) for i in dots[:-1]] + [dots[-1].lower()])
                # check for you're and other contractions
                if word_test.upper() in ["YOU'RE", "DON'T", "HAVEN'T"]:
                    return s.join([titlecase.titlecase(i, self.title_exceptions) for i in dots[:-1]] + [dots[-1].lower()])
                return s.join([titlecase.titlecase(i, self.title_exceptions) for i in dots])

        # words with no vowels in (treat as acronyms)
        vowels = re.compile("[AEIOUYaeiouy]")
        if not bool(vowels.search(word_test)):
            return word.upper()

        return None


    def parse_name(self, name):
        if not isinstance(name, str):
            return name

        name = name.strip()

        # if name is one character or less then return it
        if len(name) <= 1:
            return name

        # if it contains any lowercase letters then return as is
        for c in name:
            if c.islower():
                return name

        # try titlecasing
        try:
            name = titlecase.titlecase(name, self.title_exceptions)
        except:
            pass

        # Make sure first letter is capitalise
        return name[0].upper() + name[1:]
