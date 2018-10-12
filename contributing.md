# How to contribute

Outside help is very welcome on this project.

Important areas where help could be used include:

- adding a new spider for a data source (from the [list of potential sources](https://github.com/drkane/find-that-charity-scrapers/issues?q=is%3Aissue+is%3Aopen+label%3A%22data+source%22))
- adding tests to ensure spiders function correctly.
- improving the efficiency of existing spiders.

## Adding a new spider

Potential data sources can be found in [github issues](https://github.com/drkane/find-that-charity-scrapers/issues?q=is%3Aissue+is%3Aopen+label%3A%22data+source%22).

To scrape a data source, first create a new spider in the `findthatcharity_import/spiders`
folder (you may want to copy an existing spider). Inheriting from the `BaseScraper` class
(in `findthatcharity_import/spiders/base_scraper.py`) provides some useful utilites for
your spider.

Each spider needs the following:
- a `name` attribute (this should be the same as the file name, and ideally be one word)
- an `org_id_prefix` attribute. This will be used to construct the Org ID.

Spiders should return [`Organisation` items](/findthatcharity_import/items.py) for each 
organisation in the register. Each spider should also include at least one `Source` item,
which gives information about where the data was obtained from. See an existing spider for
the format of the source item.

We want findthatcharity to be a good internet citizen. Be respectful of the scraped sites'
capacity and bandwidth, and use the most bandwidth-efficient way of scraping the data. This
may mean using a CSV download rather than scraping individual web pages.

## Submitting changes

[Send a new pull request](https://github.com/drkane/find-that-charity-scrapers/pull/new/master)
with your changes.

## Get in touch

Report any problems or bugs by adding an issue. Get in touch by sending a tweet to 
[@kanedr](https://twitter.com/kanedr) or getting in touch by <https://drkane.co.uk/contact>.

