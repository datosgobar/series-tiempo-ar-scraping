# -*- coding: utf-8 -*-

import os
import shutil
from series_tiempo_ar import TimeSeriesDataJson

from . import TestBase
from . import MockDownloads
from . import test_files_dir
from scripts import extract_catalogs
from scripts import paths
from scripts import helpers


class TestExtractValidCatalogs(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestExtractValidCatalogs, self).__init__(*args, **kwargs)
        self._name = "valid_extraction"

    def setUp(self):
        super(TestExtractValidCatalogs, self).setUp()
        self._mocker = MockDownloads()
        self._mocker.add_url_files([
            ("https://example.com/test1.json",
                test_files_dir(self._name, "mock", "test1.json")),
            ("https://example.com/test2.xlsx",
                test_files_dir(self._name, "mock", "test2.xlsx"))
        ])
        self._mocker.start()

        extract_catalogs.main()

    def tearDown(self):
        super(TestExtractValidCatalogs, self).tearDown()
        self._mocker.stop()

    def test_extraction(self):
        """
        Probar la extracción de los catálogos listados en el índice.
        """
        found = []
        for catalog in helpers.get_catalogs_index():
            found.append(os.path.isfile(paths.get_catalog_path(catalog)))

        self.assertTrue(all(found) and found)

    def test_extraction_validates(self):
        """
        Probar que los catálogos extraídos son válidos.
        """
        valid = []
        for catalog in helpers.get_catalogs_index():
            path = paths.get_catalog_path(catalog)

            catalog = TimeSeriesDataJson(path)
            valid.append(catalog.is_valid_catalog())

        self.assertTrue(all(valid) and valid)

    def test_extraction_validates(self):
        """
        Probar que los catálogos extraídos son válidos.
        """
        valid = []
        for catalog in helpers.get_catalogs_index():
            path = paths.get_catalog_path(catalog)

            catalog = TimeSeriesDataJson(path)
            valid.append(catalog.is_valid_catalog())

        self.assertTrue(all(valid) and valid)

    def test_reports_generated(self):
        """
        Probar que los reportes de extracción de catálogos fueron generados.
        """
        found = []
        for catalog in helpers.get_catalogs_index():
            reports_path = os.path.join(paths.REPORTES_DIR, catalog)
            reports = [
                paths.EXTRACTION_MAIL_CONFIG["subject"],
                paths.EXTRACTION_MAIL_CONFIG["message"],
                paths.EXTRACTION_MAIL_CONFIG["attachments"]["errors_report"],
                paths.EXTRACTION_MAIL_CONFIG["attachments"]["datasets_report"]
            ]

            for report in reports:
                found.append(os.path.isfile(
                    os.path.join(reports_path, report)))

        self.assertTrue(all(found) and found)

    def test_xlsx_json_generated(self):
        """
        Probar que los catálogos se almacenan en formatos .json y .xlsx.
        """
        found = []
        for catalog in helpers.get_catalogs_index():
            found.append(os.path.isfile(
                paths.get_catalog_path(catalog, extension="json")))
            found.append(os.path.isfile(
                paths.get_catalog_path(catalog, extension="xlsx")))

        self.assertTrue(all(found) and found)


class TestExtractInvalidCatalogs(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestExtractInvalidCatalogs, self).__init__(*args, **kwargs)
        self._name = "invalid_extraction"

    def setUp(self):
        super(TestExtractInvalidCatalogs, self).setUp()
        self._mocker = MockDownloads()
        self._mocker.add_url_files([
            ("https://example.com/invalid_file",
                test_files_dir(self._name, "mock", "test.txt"))
        ])
        self._mocker.add_url_errors([
            ("https://example.com/invalid_url", 404)
        ])
        self._mocker.start()

    def tearDown(self):
        super(TestExtractInvalidCatalogs, self).tearDown()
        self._mocker.stop()

    def test_extraction_passes(self):
        """
        Probar que la extracción no procesa ningún catálogo.
        """
        extract_catalogs.main()

        not_exists = []

        for catalog in helpers.get_catalogs_index():
            path = paths.get_catalog_path(catalog)
            not_exists.append(not os.path.isfile(path))

        self.assertTrue(all(not_exists) and not_exists)
