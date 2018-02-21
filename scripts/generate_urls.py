#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un archivo de texto con las urls de archivos a descargar"""

from __future__ import unicode_literals
from __future__ import with_statement
import os
import codecs
import sys

from helpers import get_logger, get_catalogs_index
from pydatajson.helpers import title_to_name
from series_tiempo_ar import TimeSeriesDataJson


logger = get_logger(os.path.basename(__file__))

def get_distribution_download_urls(distributions, catalog_id):
    # agrega las url que encuentra junto con su id de catalogo
    urls = []

    for distribution in filter(lambda dist: 'downloadURL' in dist, distributions):
        if "fileName" in distribution:
            distribution_fileName = distribution["fileName"]
        else:
            distribution_fileName = "{}.{}".format(
                title_to_name(distribution["title"]),
                unicode(distribution["format"]).split("/")[-1].lower()
            )

        urls.append("{} {} {} {} {}".format(
            catalog_id,
            distribution["dataset_identifier"],
            distribution["identifier"],
            distribution_fileName,
            distribution["downloadURL"]
        ))

    return urls


def get_scraping_sources_urls(distributions, catalog_id):
    # agrega las url que encuentra junto con su id de catalogo
    urls = {
        dist['scrapingFileURL']
        for dist in distributions
        if 'scrapingFileURL' in dist and 'downloadURL' not in dist
    }

    return [
        "{} {}".format(catalog_id, source_url)
        for source_url in urls
    ]


def main(catalogs_dir, sources_type, sources_urls_path):
    urls = []

    catalog_path_template = os.path.join(catalogs_dir, "{}", "data.json")

    for catalog_id in get_catalogs_index():
        catalog_path = catalog_path_template.format(catalog_id)
        logger.info("Extrayendo URLs de fuentes de {}: {}".format(
            catalog_id, catalog_path))

        try:
            catalog = TimeSeriesDataJson(catalog_path)
            distributions = catalog.get_distributions(only_time_series=True)

            if sources_type == "scraping":
                # TODO: Agregar validaciones de scraping a series_tiempo_ar y utilizarlas
                # Reportar el error y saltear la distribucion si falla la validacion
                urls.extend(get_scraping_sources_urls(distributions, catalog_id))
            elif sources_type == "distribution":
                # TODO: Agregar mas validaciones de metadatos a series_tiempo_ar y utilizarlas
                # Reportar el error y saltear la distribucion si falla la validacion
                urls.extend(get_distribution_download_urls(distributions, catalog_id))
                
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
