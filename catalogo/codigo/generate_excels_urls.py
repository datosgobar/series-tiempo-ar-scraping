#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un archivo de texto con las urls de excels a descargar

Toma las URLs de una hoja "Parametros ETL" dentro de un archivo excel.
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
    urls_series = df.distribution_iedFileURL.unique()
    with open(excels_urls_path, "wb") as f:
        for url in urls_series:
            f.write(url)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
