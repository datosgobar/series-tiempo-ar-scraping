# -*- coding: utf-8 -*-

from . import TestBase

from scripts import helpers


class TestHelpers(TestBase):
    def __init__(self, *args, **kwargs):
        super(TestHelpers, self).__init__(*args, **kwargs)
        self._name = "helpers"

    def test_index(self):
        """
        Probar que el índice de catálogos se lee correctamente (index.yaml).
        """
        index = helpers.get_catalogs_index()
        self.assertDictEqual(index, {
            "test_json": {
                "url": "https://example.com/test1.json",
                "formato": "json"
            },
            "test_xlsx": {
                "url": "https://example.com/test2.xlsx",
                "formato": "xlsx"
            }
        })

    def test_general_config(self):
        """
        Probar que config_general.yaml se lee correctamente.
        """
        config = helpers.get_general_config()
        self.assertDictEqual(config, {
            "host": "http://localhost:8080",
            "environment": "dev",
            "logging": "info"
        })
