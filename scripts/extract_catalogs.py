#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un data.json a partir de un catálogo en excel.
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import yaml
import requests
import pandas as pd
import arrow
import logging
from openpyxl import load_workbook
from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers

from helpers import get_logger, ensure_dir_exists
from paths import SCHEMAS_DIR, REPORTES_DIR, BACKUP_CATALOG_DIR, CATALOGS_DIR
from paths import CATALOGS_INDEX_PATH

sys.path.insert(0, os.path.abspath(".."))


NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')


def read_xlsx_catalog(catalog_xlsx_path):
    """Lee catálogo en excel."""

    default_values = {
        "catalog_modified": NOW,
        "dataset_issued": NOW,
        "distribution_issued": NOW,
        "dataset_modified": NOW,
        "distribution_modified": NOW
    }
    catalogo = DataJson(catalog_xlsx_path, default_values=default_values)

    clean_catalog(catalogo)

    return catalogo


def clean_catalog(catalog):

    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if "field" in distribution:
                for field in distribution["field"]:
                    if "title" in field:
                        field["title"] = field["title"].replace(" ", "")
                    if "id" in field:
                        field["id"] = field["id"].replace(" ", "")


def write_json_catalog(catalog_id, catalog, catalog_json_path):
    """Escribe catálogo en JSON y guarda una copia con fecha."""
    catalog_backup_json_path = os.path.join(
        BACKUP_CATALOG_DIR, catalog_id, "data-{}.json".format(TODAY))

    # crea los directorios necesarios
    ensure_dir_exists(os.path.dirname(catalog_json_path))
    ensure_dir_exists(os.path.dirname(catalog_backup_json_path))

    writers.write_json_catalog(catalog, catalog_json_path)
    writers.write_json_catalog(catalog, catalog_backup_json_path)


def validate_and_filter(catalog):
    """Valida y filtra un catálogo en data.json."""
    dj = DataJson(schema_filename="catalog.json", schema_dir=SCHEMAS_DIR)

    # valida todo el catálogo para saber si está ok
    global_validation = dj.is_valid_catalog(catalog)
    logging.info(
        "Metadata a nivel de catálogo es válida? {}".format(global_validation))

    # genera reportes de validación
    validation_result = dj.validate_catalog(catalog)
    writers.write_json(
        validation_result,
        os.path.join(REPORTES_DIR, "reporte-catalogo-completo.json")
    )
    datasets_errors = filter(lambda x: x["status"] == "ERROR",
                             validation_result["error"]["dataset"])

    writers.write_json(
        datasets_errors,
        os.path.join(REPORTES_DIR, "reporte-datasets-error.json")
    )
    # genera catálogo filtrado por los datasets que no tienen error

    dj.generate_datasets_report(
        catalog, harvest='valid',
        export_path=os.path.join(REPORTES_DIR, "reporte-datasets.xlsx")
    )
    catalog_filtered = dj.generate_harvestable_catalogs(
        catalog, harvest='valid')[0]

    return catalog_filtered


def process_catalog(catalog_id, catalog_format, catalog_url,
                    catalogs_dir=CATALOGS_DIR):
    """Descarga y procesa el catálogo.

    Transforma catálogos en distintos formatos a data.json, valida y actualiza
    algunos campos de metadatos y emite informes a los administradores.

    Args:
        catalog_id (str): Identificador del catálogo.
        catalog_format (str): Uno de "xlsx", "json" o "ckan".
        catalog_url (str): Url pública desde donde descargar el catálogo.
        catalogs_dir (str): Directorio local donde se descargan los catálogos.
    """
    logger = get_logger(__name__)

    # crea directorio y template de path al catálogo y reportes
    catalog_dir = os.path.join(catalogs_dir, catalog_id)
    ensure_dir_exists(catalog_dir)
    catalog_path_template = os.path.join(catalog_dir, "{}")

    # procesa el catálogo dependiendo del formato
    logger.info('=== Catálogo {} ==='.format(catalog_id.upper()))
    try:
        logger.info('- Descarga y lectura de catálogo')
        if catalog_format.lower() == 'xlsx':

            # descarga del catálogo
            res = requests.get(catalog_url, verify=False)
            catalog_xlsx_path = catalog_path_template.format("catalog.xlsx")
            with open(catalog_xlsx_path, 'w') as f:
                f.write(res.content)

            logger.info('- Transformación de XLSX a JSON')
            catalog = read_xlsx_catalog(catalog_xlsx_path)

        elif catalog_format.lower() == 'json':
            logger.info('- Lectura directa de JSON')
            catalog = DataJson(catalog_url)

        elif catalog_format.lower() == 'ckan':
            logger.info('- Transformación de CKAN API a JSON')
            catalog = read_ckan_catalog(catalog_url)

        else:
            raise ValueError(
                '{} no es una extension valida para un catalogo.'.format(
                    file_ext))

        # filtra, valida y escribe el catálogo en JSON y XLSX
        if catalog and len(catalog) > 0:
            logger.info("- Valida y filtra el catálogo")
            catalog_filtered = validate_and_filter(catalog)

            logger.info("- Setea el draft status de todas las distribuciones")
            for distribution in catalog.get_distributions():
                distribution["draft"] = False

            logger.info('- Escritura de catálogo en JSON')
            write_json_catalog(
                catalog_id, catalog_filtered,
                catalog_path_template.format("data.json"))

            # logger.info('- Escritura de catálogo en XLSX')
            # writers.write_xlsx_catalog(
            # catalog_filtered, catalog_path_template.format("catalog.xlsx"))
        else:
            raise Exception("El catálogo {} no se pudo generar".format(
                catalog_id))

        # genera reportes del catálogo
        logger.info('- Generación de reportes')
        catalog_filtered.generate_catalog_readme(
            catalog_filtered,
            export_path=catalog_path_template.format('README.md'))
        catalog_filtered.generate_datasets_summary(
            catalog_filtered,
            export_path=catalog_path_template.format('datasets.csv'))

    except Exception as e:
        logger.error(
            'Error al procesar el catálogo de {}'.format(catalog_id),
            exc_info=True)


def main(catalogs_index_path=CATALOGS_INDEX_PATH, catalogs_dir=CATALOGS_DIR):
    logger = get_logger(__name__)

    logger.info('>>> COMIENZO DE LA EXTRACCION DE CATALOGOS <<<')

    # cargo los parámetros de los catálogos a extraer
    with open(catalogs_index_path) as config_file:
        catalogs_params = yaml.load(config_file)

    # procesa los catálogos
    for catalog_id in catalogs_params:
        process_catalog(
            catalog_id,
            catalogs_params[catalog_id]["formato"],
            catalogs_params[catalog_id]["url"],
            catalogs_dir
        )

    logger.info('>>> FIN DE LA EXTRACCION DE CATALOGOS <<<')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
