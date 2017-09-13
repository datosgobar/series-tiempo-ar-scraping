#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scrapea datasets a partir de un catálogo y parámetros para XlSeries
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import arrow
import pydatajson
import json

from data import get_series_dict, generate_series_jsons


sys.path.insert(0, os.path.abspath(".."))


def main(catalog_json_path, series_params_path, catalogs_dir, series_dir):

    catalog = pydatajson.DataJson(catalog_json_path)

    with open(series_params_path, "r") as f:
        series_params = json.load(f, encoding='utf-8')

    # genera series como archivos individuales en JSON
    ts_dict = get_series_dict(
        catalog, series_params["series_ids_included"],
        catalogs_dir=catalogs_dir,
        metadata_included=series_params["metadata_fields_included"],
        debug_mode=False
    )
    generate_series_jsons(ts_dict, series_dir)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
