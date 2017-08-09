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

from helpers import get_logger
from data import get_time_series_data

sys.path.insert(0, os.path.abspath(".."))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
DATASETS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "datasets")
DUMPS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "dumps")
DUMPS_PARAMS_PATH = os.path.join(
    PROJECT_DIR, "catalogo", "datos", "dumps_params.json"
)
CATALOG_PATH = os.path.join(PROJECT_DIR, "catalogo", "datos", "data.json")


# TODO: agregar al dump
# theme_label
# dataset_title
# distribution_title
# dataset_identifier

def main(catalog_json_path=CATALOG_PATH, dumps_params_path=DUMPS_PARAMS_PATH,
         datasets_dir=DATASETS_DIR, dumps_dir=DUMPS_DIR):
    logger = get_logger(__name__)

    with open(dumps_params_path, "r") as f:
        dumps_params = json.load(f, encoding='utf-8')

    for dump_file_name, dump_params in dumps_params.iteritems():
        dump_path = os.path.join(dumps_dir, dump_file_name)

        # genera un dump de series de tiempo
        df = get_time_series_data(
            dump_params, catalog_json_path, dump_path,
            datasets_dir=datasets_dir
        )
        logger.info("\n{}  Series: {} Values: {}".format(
            dump_file_name.ljust(40),
            unicode(len(df.field_id.unique())).ljust(3),
            unicode(len(df)).ljust(6)
        ))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
