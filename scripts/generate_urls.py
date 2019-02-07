"""Genera un archivo de texto con las urls de archivos a descargar"""

import os
import codecs
import sys
from pydatajson.helpers import title_to_name
from series_tiempo_ar import TimeSeriesDataJson
from paths import DIST_URLS_PATH, SCRAP_URLS_PATH
from paths import get_catalog_path
from helpers import get_logger, get_catalogs_index, print_log_separator

logger = get_logger(os.path.basename(__file__))


def get_distribution_download_urls(distributions, catalog_id):
    # agrega las url que encuentra junto con su id de catalogo
    urls = []

    url_dists = [
        dist for dist in distributions
        if 'downloadURL' in dist and dist['downloadURL']
    ]

    for distribution in url_dists:
        if "fileName" in distribution:
            distribution_fileName = distribution["fileName"]
        else:
            distribution_fileName = "{}.{}".format(
                title_to_name(distribution["title"]),
                str(distribution["format"]).split("/")[-1].lower()
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
        if 'scrapingFileURL' in dist and (
            'downloadURL' not in dist or not dist['downloadURL']
        )
    }

    return [
        "{} {}".format(catalog_id, source_url)
        for source_url in urls
    ]


def main(sources_type):
    urls = []
    print_log_separator(logger, "Extracción de URLS para: {}".format(
        sources_type))

    if sources_type == "scraping":
        sources_urls_path = SCRAP_URLS_PATH
    elif sources_type == "distribution":
        sources_urls_path = DIST_URLS_PATH

    for catalog_id in get_catalogs_index():
        catalog_path = get_catalog_path(catalog_id)

        try:
            catalog = TimeSeriesDataJson(catalog_path)
            distributions = catalog.get_distributions(only_time_series=True)

            if sources_type == "scraping":
                logger.info("Extrayendo URLs de fuentes de {}...".format(
                    catalog_id))

                # TODO: Agregar validaciones de scraping a series_tiempo_ar
                # y utilizarlas.
                # Reportar el error y saltear la distribucion si falla la
                # validacion.
                urls.extend(get_scraping_sources_urls(
                    distributions, catalog_id))
            elif sources_type == "distribution":
                logger.info(
                    "Extrayendo URLs de distribuciones de {}...".format(
                        catalog_id))
                # TODO: Agregar mas validaciones de metadatos a
                # series_tiempo_ar y utilizarlas.
                # Reportar el error y saltear la distribucion si falla la
                # validacion.
                urls.extend(get_distribution_download_urls(
                    distributions, catalog_id))

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "No se pudo extraer URLs de fuentes del catalogo {}".format(
                    catalog_id))
            logger.error(e)

    logger.info("{} URLs de {} en total".format(len(urls), sources_type))

    with codecs.open(sources_urls_path, "wb", encoding="utf-8") as f:
        f.write("\n".join(urls))
        f.write("\n")


if __name__ == '__main__':
    main(sys.argv[1])
