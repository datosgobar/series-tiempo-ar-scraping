#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un data.json a partir de un catálogo en excel.
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import pandas as pd
import arrow
import logging
from openpyxl import load_workbook
from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers

from helpers import get_logger

sys.path.insert(0, os.path.abspath(".."))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
LOGS_DIR = os.path.join(PROJECT_DIR, "catalogo", "logs")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
REPORTES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "reportes")

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')


def read_xlsx_catalog(catalog_xlsx_path):
    """Lee catálogo en excel."""

    default_values = {
        "dataset_issued": NOW,
        "distribution_issued": NOW
    }
    catalogo = readers.read_catalog(
        catalog_xlsx_path, default_values=default_values)

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


def write_json_catalog(catalog, catalog_json_path):
    """Escribe catálogo en JSON y guarda una copia con fecha."""
    dir_datos = os.path.dirname(catalog_json_path)

    writers.write_json_catalog(catalog, catalog_json_path)
    writers.write_json_catalog(catalog, os.path.join(
        dir_datos, "catalogos", "data-{}.json".format(TODAY))
    )


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


def main(catalog_xlsx_path, catalog_json_path):
    logger = get_logger(__name__)

    logger.info("Comienza a leer {}".format(catalog_xlsx_path))
    catalog = read_xlsx_catalog(catalog_xlsx_path)
    logger.info("Termina de leer {}".format(catalog_xlsx_path))

    logger.info("Escribe catálogo original {}".format(catalog_json_path))
    write_json_catalog(catalog, catalog_json_path)

    logger.info("Valida y filtra el catálogo")
    catalog_filtered = validate_and_filter(catalog)

    logger.info("Setea el draft status de todas las distribuciones")
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            distribution["draft"] = False

    logger.info("Escribe catálogo filtrado {}".format(catalog_json_path))
    write_json_catalog(catalog_filtered, catalog_json_path)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
