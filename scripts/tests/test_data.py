#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""test_data.py

Tests for `data.py` module.
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import unittest
import nose
import sys

from context import data, paths

CATALOGS_DIR = paths.TEST_CATALOGS_DIR


class GetDataTestCase(unittest.TestCase):
    """Tests for GetData class."""

    def test_get_series(self):
        serie = data.get_series("sspm", "1", "1.1", "oferta_global_pbi",
                                fmt="serie")

        self.assertEqual(len(serie), 20)
        self.assertEqual(serie[-1], 468301.014942)

    # @unittest.skip("skip")
    def test_get_series_by_id(self):
        serie = data.get_series_by_id("1.1_OGP_D_1993_A_17", fmt="serie")

        self.assertEqual(len(serie), 20)
        self.assertEqual(serie[-1], 468301.014942)

    # @unittest.skip("skip")
    def test_get_series_pct_change(self):
        serie = data.get_series("sspm", "1", "1.1", "oferta_global_pbi",
                                fmt="serie", pct_change=1)

        self.assertEqual(len(serie), 20)
        self.assertEqual(round(serie[-1], 6), 0.018996)


if __name__ == '__main__':
    nose.run(defaultTest=__name__)
