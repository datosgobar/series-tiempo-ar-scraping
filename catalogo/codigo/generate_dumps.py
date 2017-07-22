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

from data import get_time_series_data


sys.path.insert(0, os.path.abspath(".."))


def main(catalog_json_path, dumps_params_path, datasets_dir, dump_path):

    with open(dumps_params_path, "r") as f:
        dumps_params = json.load(f, encoding='utf-8')

    dump_file_name = os.path.basename(dump_path)

    # genera un dump de series de tiempo
    df = get_time_series_data(
        dumps_params[dump_file_name], catalog_json_path, dump_path,
        datasets_dir=datasets_dir
    )
    print("{} values".format(len(df)))
    print("{} series".format(len(df.field_id.unique())))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
