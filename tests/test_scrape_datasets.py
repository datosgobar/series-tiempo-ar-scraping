# -*- coding: utf-8 -*-

import os
from series_tiempo_ar import TimeSeriesDataJson
from scripts import scrape_datasets
from scripts import extract_catalogs
from scripts import paths
from scripts import helpers
from . import TestBase
from . import MockDownloads
from . import test_files_dir


class TestScrapeDatasetsTextFiles(TestBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = "scrape_datasets_text_files"

    def setUp(self):
        super().setUp()
        self._mocker = MockDownloads()
        self._mocker.add_url_files([
            ("http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/panser.txt",
             test_files_dir(self._name, "mock", "panser.txt"))
        ])
        # self._mocker.start()

    def tearDown(self):
        super().tearDown()
        # self._mocker.stop()

    def test_distributions_created(self):
        """
        Probar que todas las distribuciones del catálogo se encuentran
        descargadas, ya sea directamente o a través de scraping.
        """
        print(paths.get_catalog_path("bcra"))
        catalog = TimeSeriesDataJson(
            paths.get_catalog_path("bcra"))
        distributions = catalog.get_distributions(only_time_series=True)
        scrape_datasets.main(True)

        found = []
        for distribution in distributions:
            filepath = paths.get_distribution_path(
                catalog["identifier"],
                distribution["dataset_identifier"],
                distribution["identifier"]
            )
            print(filepath)
            found.append(os.path.isfile(filepath))

        self.assertTrue(all(found) and found)

    def test_distributions_download_url(self):
        """
        Probar que todas las distribuciones del catálogo tienen downloadURL.
        """
        scrape_datasets.main(True)

        catalog = TimeSeriesDataJson(
            paths.get_catalog_path("bcra"))
        distributions = catalog.get_distributions(only_time_series=True)

        valid = []
        for distribution in distributions:
            valid.append("downloadURL" in distribution)

        self.assertTrue(all(valid) and valid)


class TestScrapeDatasets(TestBase):

    def __init__(self, *args, **kwargs):
        super(TestScrapeDatasets, self).__init__(*args, **kwargs)
        self._name = "scrape_datasets"

    def test_scraping_successful_no_replace(self):
        """
        Probar que no se lanzan excepciones cuando se scrapean las
        distribuciones utilizando replace=False.
        """
        # El test pasa si no se lanza una excepción
        scrape_datasets.main(False)

    def test_distributions_created(self):
        """
        Probar que todas las distribuciones del catálogo se encuentran
        descargadas, ya sea directamente o a través de scraping.
        """
        catalog = TimeSeriesDataJson(
            paths.get_catalog_path("example_catalog1"))
        distributions = catalog.get_distributions(only_time_series=True)

        scrape_datasets.main(True)

        found = []
        for distribution in distributions:
            filepath = paths.get_distribution_path(
                catalog["identifier"],
                distribution["dataset_identifier"],
                distribution["identifier"]
            )
            found.append(os.path.isfile(filepath))

        self.assertTrue(all(found) and found)

    def test_distributions_download_url(self):
        """
        Probar que todas las distribuciones del catálogo tienen downloadURL.
        """
        scrape_datasets.main(True)

        catalog = TimeSeriesDataJson(
            paths.get_catalog_path("example_catalog1"))
        distributions = catalog.get_distributions(only_time_series=True)

        valid = []
        for distribution in distributions:
            valid.append("downloadURL" in distribution)

        self.assertTrue(all(valid) and valid)

    def test_reports_generated(self):
        """
        Probar que los reportes de scraping fueron generados.
        """
        scrape_datasets.main(True)

        found = []
        for catalog in helpers.get_catalogs_index():
            reports_path = os.path.join(paths.REPORTES_DIR, catalog)
            reports = [
                paths.SCRAPING_MAIL_CONFIG["subject"],
                paths.SCRAPING_MAIL_CONFIG["message"],
                paths.SCRAPING_MAIL_CONFIG["attachments"]["files_report"],
                paths.SCRAPING_MAIL_CONFIG["attachments"]["datasets_report"],
                paths.SCRAPING_MAIL_CONFIG["attachments"][
                    "distributions_report"]
            ]

            for report in reports:
                found.append(
                    os.path.isfile(os.path.join(reports_path, report)))

        self.assertTrue(all(found) and found)


class TestScrapeDatasetsMissingFiles(TestBase):

    def __init__(self, *args, **kwargs):
        super(TestScrapeDatasetsMissingFiles, self).__init__(*args, **kwargs)
        self._name = "scrape_datasets_missing_files"

    def test_scraping_successful(self):
        """
        Probar que las distribuciones con downloadURL fueron descargadas. Las
        distribuciones a ser scrapeadas cuyos archivos no pudieron ser
        descargados se ignoran.
        """
        scrape_datasets.main(True)

        # Leer catálogo descargado originalmente
        catalog = TimeSeriesDataJson(paths.get_catalog_path(
            "example_catalog1",
            catalogs_dir=paths.CATALOGS_DIR_INPUT,
            extension="xlsx"
        ))
        distributions = catalog.get_distributions(only_time_series=True)

        found = []
        for distribution in distributions:
            if "downloadURL" in distribution:
                # Las distribuciones con campo downloadURL deberían haber sido
                # descargadas.
                filepath = paths.get_distribution_path(
                    catalog["identifier"],
                    distribution["dataset_identifier"],
                    distribution["identifier"]
                )
                found.append(os.path.isfile(filepath))

        self.assertTrue(all(found) and found)
