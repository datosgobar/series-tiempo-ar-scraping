# -*- coding: utf-8 -*-

from scripts import generate_urls
from scripts import paths
from . import TestBase


class TestGenerateUrls(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestGenerateUrls, self).__init__(*args, **kwargs)
        self._name = "generate_urls"

    def test_scraping_urls_generated(self):
        """
        Probar que se extraen URLs para scraping.
        """
        generate_urls.main("scraping")
        with open(paths.SCRAP_URLS_PATH) as f:
            scrap_urls = [l.strip() for l in f.readlines() if l.strip()]

        self.assertTrue(scrap_urls)

    def test_distribution_urls_generated(self):
        """
        Probar que se extraen URLs para descarga de distribuciones.
        """
        generate_urls.main("distribution")
        with open(paths.DIST_URLS_PATH) as f:
            dist_urls = [l.strip() for l in f.readlines() if l.strip()]

        self.assertTrue(dist_urls)
