from __future__ import print_function
import logging

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.commands.crawl import Command as CrawlCommand

class Command(CrawlCommand):

    def short_desc(self):
        return "Run all spiders"

    def run(self, args, opts):

        runner = CrawlerRunner(self.settings)

        for spname in sorted(self.crawler_process.spider_loader.list()):
            logging.info("Starting spider: %s", spname)
            runner.crawl(spname, **opts.spargs)

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
