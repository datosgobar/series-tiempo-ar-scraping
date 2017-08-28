#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un archivo de texto con las urls de archivos a descargar"""

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

    # ignora los archivos excel abiertos
    catalog_xslx_paths = [
        excel_file for excel_file
        in glob.glob(os.path.join(catalogs_dir, "*", "*.xlsx"))
        if "~$" not in excel_file
    ]
    for catalog_xlsx_path in catalog_xslx_paths:
        catalog_id = os.path.basename(os.path.dirname(catalog_xlsx_path))
        print("Extrayendo URLs de fuentes de {}: {}".format(
            catalog_id, catalog_xlsx_path))

        df = pd.read_excel(
            catalog_xlsx_path,
            find_ws_name(catalog_xlsx_path, DISTRIBUTION_SHEET_NAME)
        )

        # agrega primero las URLs de fuentes a scrapear
        urls_series.extend(
            # agrega las url que encuentra junto con su id de catalogo
            ["{} {}".format(catalog_id, source_url)
             for source_url in df.distribution_iedFileURL.unique()
             if pd.notnull(source_url)]
        )

        # agrega después las URLs de distribuciones ya formadas
        urls_series.extend(
            # agrega las url que encuentra junto con su id de catalogo
            ["{} {}".format(catalog_id, source_url)
             for source_url in df.distribution_downloadURL.unique()
             if pd.notnull(source_url)]
        )

    print("{} URLs de fuentes en total".format(len(urls_series)))

    with open(sources_urls_path, "wb") as f:
        f.write("\n".join(urls_series))
        f.write("\n")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
