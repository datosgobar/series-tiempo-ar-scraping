#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un CSV con los parámetros necesarios para parsear excels de IED
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys

import pandas as pd

PARAMS_SHEET_NAME = "Parametros ETL"


def main(catalog_xlsx_path, excels_urls_path):
    df = pd.read_excel(catalog_xlsx_path, PARAMS_SHEET_NAME)
    params_fields = [
        "distribution_iedFileURL", "distribution_iedFileSheet",
        "distribution_timeIndexCell", "distribution_identifier",
        "distribution_title", "field_title", "field_id",
        "field_identifierCell", "field_dataStartCell"
    ]
    df[params_fields].to_csv(
        "catalogo/datos/etl_params.csv",
        encoding="utf8", index=False
    )


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
