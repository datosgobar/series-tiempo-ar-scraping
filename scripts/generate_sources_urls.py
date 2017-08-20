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
import glob

import pandas as pd
from helpers import find_ws_name

DISTRIBUTION_SHEET_NAME = "distribution"


def main(catalogs_dir, sources_urls_path):
    urls_series = []

    catalog_xslx_paths = glob.glob(os.path.join(catalogs_dir, "*", "*.xlsx"))
    for catalog_xlsx_path in catalog_xslx_paths:
        catalog_id = os.path.basename(os.path.dirname(catalog_xlsx_path))

        df = pd.read_excel(
            catalog_xlsx_path,
            find_ws_name(catalog_xlsx_path, DISTRIBUTION_SHEET_NAME)
        )
        urls_series.extend(
            # agrega las url que encuentra junto con su id de catalogo
            ["{} {}".format(catalog_id, source_url)
             for source_url in df.distribution_iedFileURL.unique()]
        )

    with open(sources_urls_path, "wb") as f:
        f.write("\n".join(urls_series))
        f.write("\n")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
