#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un archivo de texto con las urls de archivos a descargar"""

from __future__ import unicode_literals
from __future__ import with_statement
import os
import codecs
import sys
import glob
from pydatajson.helpers import title_to_name

import pandas as pd
from helpers import find_ws_name, get_logger
from data import get_time_index_field

DISTRIBUTION_SHEET_NAME = "distribution"

logger = get_logger(os.path.basename(__file__))

def get_distribution_download_urls(df, catalog_id):
    # agrega las url que encuentra junto con su id de catalogo
    urls = []
    df_no_scraping = df[pd.notnull(df.distribution_downloadURL)]

    for index, row in df_no_scraping.iterrows():

        # tomo el nombre del archivo, si está, o lo genero
        if ("distribution_fileName" in row and row["distribution_fileName"]
                and pd.notnull(row["distribution_fileName"])):
            distribution_fileName = row["distribution_fileName"]
        else:
            distribution_fileName = "{}.{}".format(
                title_to_name(row["distribution_title"]),
                unicode(row["distribution_format"]).split("/")[-1].lower()
            )

        # solo agrega urls que tengan series de tiempo
        if catalog_id != "modernizacion":
            urls.append("{} {} {} {} {}".format(
                catalog_id, row["dataset_identifier"],
                row["distribution_identifier"],
                distribution_fileName, row["distribution_downloadURL"]
            ))

    return urls


def get_scraping_sources_urls(df, catalog_id):
    # agrega las url que encuentra junto con su id de catalogo
    return [
        "{} {}".format(catalog_id, source_url)
        for source_url in df.distribution_scrapingFileURL.unique()
        if pd.notnull(source_url)
    ]


def main(catalogs_dir, sources_type, sources_urls_path):
    urls = []

    # ignora los archivos excel abiertos
    catalog_xslx_paths = [
        excel_file for excel_file
        in glob.glob(os.path.join(catalogs_dir, "*", "*.xlsx"))
        if "~$" not in excel_file
    ]
    for catalog_xlsx_path in catalog_xslx_paths:
        catalog_id = os.path.basename(os.path.dirname(catalog_xlsx_path))
        logger.info("Extrayendo URLs de fuentes de {}: {}".format(
            catalog_id, catalog_xlsx_path))

        try:
            df = pd.read_excel(
                catalog_xlsx_path,
                find_ws_name(catalog_xlsx_path, DISTRIBUTION_SHEET_NAME),
                dtype={
                    "dataset_identifier": object,
                    "dataset_title": object,
                    "distribution_identifier": object,
                    "distribution_title": object
                }
            )

            if sources_type == "scraping":
                if "distribution_scrapingFileURL" in df.columns:
                    urls.extend(get_scraping_sources_urls(df, catalog_id))
                else:
                    logger.info("Nada que scrapear en el catalogo: {}".format(
                        catalog_id))
            elif sources_type == "distribution":
                urls.extend(get_distribution_download_urls(df, catalog_id))
            else:
                raise Exception("No se reconoce el tipo de fuente {}".format(
                    sources_type))
        except Exception as e:
            logger.error("No se pudo extraer URLs de fuentes del catalogo {}".format(
                catalog_id))
            logger.error(e)

    logger.info("{} URLs de {} en total".format(len(urls), sources_type))

    with codecs.open(sources_urls_path, "wb", encoding="utf-8") as f:
        f.write("\n".join(urls))
        f.write("\n")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
