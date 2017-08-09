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

import data

DATASETS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "datasets_test")


class GetDataTestCase(unittest.TestCase):
    """Tests for GetData class."""

    def test_get_time_series(self):
        serie = data.get_time_series("1", "1.1", "oferta_global_pbi",
                                     fmt="serie")

        self.assertEqual(len(serie) == 20)
        self.assertEqual(serie[-1] == 468301.014942)

    def test_get_time_series_pct_change(self):
        serie = data.get_time_series("1", "1.1", "oferta_global_pbi",
                                     fmt="serie", pct_change=1)

        self.assertEqual(len(serie) == 20)
        self.assertEqual(serie[-1] == 0.018996)


if __name__ == '__main__':
    nose.run(defaultTest=__name__)
