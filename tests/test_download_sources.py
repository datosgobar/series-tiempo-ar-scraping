# -*- coding: utf-8 -*-

import shutil
import os

from . import TestBase
from . import MockDownloads
from . import test_files_dir

from scripts import download_urls
from scripts import paths


class TestDownloadSources(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestDownloadSources, self).__init__(*args, **kwargs)
        self._name = "download_sources"

    def setUp(self):
        super(TestDownloadSources, self).setUp()
        self._mocker = MockDownloads()
        self._mocker.start()

    def tearDown(self):
        super(TestDownloadSources, self).tearDown()
        self._mocker.stop()

    def test_scraping_sources_downloaded(self):
        """
        Probar que se descargan todos los archivos de scraping, incluso cuando
        algunas URLs son inválidas.
        """
        self._mocker.add_url_files([
            ("https://example.com/file1.xlsx", test_files_dir(self._name, "mock", "test.xlsx")),
            ("https://example.com/file2.xlsx", test_files_dir(self._name, "mock", "test.xlsx"))
        ])
        self._mocker.add_url_errors([
            ("https://example.com/invalid_url", 404)
        ])

        download_urls.main("scraping")

        found = []
        with open(paths.SCRAP_URLS_PATH) as f:
            for line in f.readlines():
                catalog = line.strip().split()[0]
                found.append(os.path.isdir(paths.get_catalog_scraping_sources_dir(catalog)))

        self.assertTrue(all(found) and found)

    def test_distribution_sources_downloaded(self):
        """
        Probar que se descargan todos los archivos de distribuciones, incluso
        cuando algunas URLs son inválidas.
        """
        self._mocker.add_url_files([
            ("https://example.com/file1.csv", test_files_dir(self._name, "mock", "test.csv")),
            ("https://example.com/file2.csv", test_files_dir(self._name, "mock", "test.csv"))
        ])
        self._mocker.add_url_errors([
            ("https://example.com/invalid_url", 404)
        ])

        download_urls.main("distribution")

        found = []
        with open(paths.DIST_URLS_PATH) as f:
            for line in f.readlines():
                catalog, dataset, distribution, filename, _ = line.strip().split()
                
                filepath = os.path.join(paths.get_distribution_download_dir(
                    paths.CATALOGS_DIR_INPUT, catalog, dataset, distribution
                ), filename)

                found.append(os.path.isfile(filepath))

        self.assertTrue(all(found) and found)

    def test_no_scraping_sources(self):
        """
        Probar que no se lanzan excepciones si el archivo de URLs de scraping
        no existe.
        """
        os.remove(paths.SCRAP_URLS_PATH)
        # El test pasa si no se lanza una excepción
        download_urls.main("scraping")

    def test_empty_scraping_sources(self):
        """
        Probar que no se lanzan excepciones si el archivo de URLs de scraping
        está vacío.
        """
        # Truncar archivo
        with open(paths.SCRAP_URLS_PATH, "w") as f:
            pass

        # El test pasa si no se lanza una excepción
        download_urls.main("scraping")

    def test_no_distribution_sources(self):
        """
        Probar que no se lanzan excepciones si el archivo de URLs de distribuciones
        no existe.
        """
        os.remove(paths.DIST_URLS_PATH)
        # El test pasa si no se lanza una excepción
        download_urls.main("distribution")

    def test_empty_distribution_sources(self):
        """
        Probar que no se lanzan excepciones si el archivo de URLs de distribuciones
        está vacío.
        """
        # Truncar archivo
        with open(paths.DIST_URLS_PATH, "w") as f:
            pass

        # El test pasa si no se lanza una excepción
        download_urls.main("distribution")

