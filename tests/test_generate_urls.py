# -*- coding: utf-8 -*-

import shutil
import os

from . import TestBase
from . import test_files_dir

from scripts import generate_urls
from scripts import paths


class TestGenerateUrls(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestGenerateUrls, self).__init__(*args, **kwargs)
        self._name = "generate_urls"

    def generate_urls(self):
        generate_urls.main("distribution")
        generate_urls.main("scraping")

    def test_urls_generated(self):
        self.generate_urls()
        with open(paths.DIST_URLS_PATH) as f:
            dist_urls = [l.strip() for l in f.readlines() if l.strip()]

        with open(paths.SCRAP_URLS_PATH) as f:
            scrap_urls = [l.strip() for l in f.readlines() if l.strip()]

        self.assertTrue(dist_urls and scrap_urls)