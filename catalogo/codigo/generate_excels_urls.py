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
from helpers import find_ws_name

DISTRIBUTION_SHEET_NAME = "distribution"


def main(catalog_xlsx_path, excels_urls_path):
    df = pd.read_excel(
        catalog_xlsx_path,
        find_ws_name(catalog_xlsx_path, DISTRIBUTION_SHEET_NAME)
    )
    urls_series = df.distribution_iedFileURL.unique()

    with open(excels_urls_path, "wb") as f:
        f.write("\n".join(urls_series))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
