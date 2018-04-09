#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import with_statement
import os
import codecs
import sys
import urlparse

from paths import CATALOGS_DIR_INPUT, DIST_URLS_PATH, SCRAP_URLS_PATH
from paths import get_distribution_download_dir
from paths import get_catalog_scraping_sources_dir
from helpers import get_logger, ensure_dir_exists, print_log_separator
from helpers import download_with_config, get_catalog_download_config

logger = get_logger(os.path.basename(__file__))

def download_scraping_sources(urls):
    for entry in urls:
        catalog_id, scraping_url = entry.split()
        
        config = get_catalog_download_config(catalog_id)["sources"]
        
        logger.info("Descargando archivo de scraping para catalogo: {}".format(
            catalog_id
        ))

        logger.info("URL: {}".format(scraping_url))
        logger.info("Comenzando...")

        catalog_scraping_sources_dir = get_catalog_scraping_sources_dir(
            catalog_id
        )

        ensure_dir_exists(catalog_scraping_sources_dir)
        url = urlparse.urlparse(scraping_url)
        file = os.path.basename(url.path)
        file_path = os.path.join(catalog_scraping_sources_dir, file)

        try:
            download_with_config(scraping_url, file_path, config)
            logger.info("Archivo descargado")
        except Exception as e:
            logger.error("Error al descargar el archivo")
            logger.error(e)


def download_distributions(urls):
    for entry in urls:
        parts = entry.split()
        catalog_id, dataset_id, distribution_id, filename, url = parts

        config = get_catalog_download_config(catalog_id)["sources"]
        
        logger.info(
            "Descargando archivo de distribucion: {} (catálogo {})".format(
            distribution_id,
            catalog_id
        ))

        logger.info("URL: {}".format(url))
        logger.info("Comenzando...")

        distribution_download_dir = get_distribution_download_dir(
            CATALOGS_DIR_INPUT,
            catalog_id,
            dataset_id,
            distribution_id
        )

        ensure_dir_exists(distribution_download_dir)
        file_path = os.path.join(distribution_download_dir, filename)

        try:
            download_with_config(url, file_path, config)
            logger.info("Archivo descargado")
        except Exception as e:
            logger.error("Error al descargar el archivo")
            logger.error(e)


def main(sources_type):
    if sources_type == "scraping":
        sources_urls_path = SCRAP_URLS_PATH
    elif sources_type == "distribution":
        sources_urls_path = DIST_URLS_PATH

    with codecs.open(sources_urls_path, "rb") as f:
        urls = f.readlines()

    print_log_separator(logger, "Descarga de fuentes: {}".format(sources_type))
    logger.info("# URLS: {}".format(len(urls)))

    if sources_type == "scraping":
        download_scraping_sources(urls)
    elif sources_type == "distribution":
        download_distributions(urls)

    logger.info("Descargas finalizadas para {}.".format(sources_type))


if __name__ == '__main__':
    main(sys.argv[1])
