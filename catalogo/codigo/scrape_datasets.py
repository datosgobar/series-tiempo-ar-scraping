#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scrapea datasets a partir de un catálogo y parámetros para XlSeries
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import pandas as pd
import arrow
from openpyxl import load_workbook
import logging
from copy import deepcopy

from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers
from helpers import row_from_cell_coord

sys.path.insert(0, os.path.abspath(".."))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
LOGS_DIR = os.path.join(PROJECT_DIR, "catalogo", "logs")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
REPORTES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "reportes")

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, 'scrape_datasets.log'),
    filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s'
)

XLSERIES_PARAMS = {
    'alignment': u'vertical',
    'composed_headers_coord': None,
    'context': None,
    'continuity': True,
    'blank_rows': False,
    'missings': True,
    'missing_value': [None, '-', '...', '.', '/', '///', '', "s.d."],
    'time_alignment': 0,
    'time_multicolumn': False,
    "headers_coord": None,
    "data_starts": None,
    "frequency": None,
    "time_header_coord": None,
    'time_composed': True
}


def get_dataset_metadata(catalog, dataset_identifier):
    datasets = filter(lambda x: x.get("identifier") == dataset_identifier,
                      catalog["dataset"])
    msg = "Hay más de 1 dataset con id {}: {}".format(dataset_identifier,
                                                      datasets)
    assert len(datasets) == 1, msg

    return datasets[0]


def get_distribution_metadata(catalog, distribution_identifier,
                              dataset_identifier=None):

    dataset_identifier = (dataset_identifier or
                          distribution_identifier.split(".")[0])
    dataset = get_dataset_metadata(catalog, dataset_identifier)

    distributions = filter(
        lambda x: x.get("identifier") == distribution_identifier,
        dataset["distribution"])
    msg = "Hay más de 1 distribution con id {}: {}".format(
        distribution_identifier, distributions)
    assert len(distributions) == 1, msg

    return distributions[0]


def get_field_metadata(catalog, distribution_identifier, field_id=None,
                       field_title=None, dataset_identifier=None):

    distribution = get_distribution_metadata(catalog, distribution_identifier,
                                             dataset_identifier)

    assert field_id or field_title, "Se necesita el título o id del campo."

    if field_id:
        fields = filter(lambda x: x.get("id") == field_id,
                        distribution["field"])
        msg = "Hay más de 1 field con id {}: {}".format(field_id, fields)
        assert len(fields) == 1, msg

    else:
        fields = filter(lambda x: x.get("title") == field_title,
                        distribution["field"])
        msg = "Hay más de 1 field con título {}: {}".format(field_title,
                                                            fields)
        assert len(fields) == 1, msg

    return fields[0]


def _convert_frequency(freq_iso8601):
    frequencies_map = {
        "R/P1Y": "Y",
        "R/P3M": "Q",
        "R/P1M": "M"
    }
    return frequencies_map[freq_iso8601]


def gen_distribution_params(etl_params, catalog, distribution_identifier):
    df_distrib = etl_params[
        etl_params.distribution_identifier == distribution_identifier
    ]
    df_fields = df_distrib[etl_params.field_title != "indice_tiempo"]

    num_series = len(df_fields)
    params = {}

    # hoja de la distribución
    worksheets = list(df_distrib.distribution_iedFileSheet.unique())
    msg = "Distribución {} en más de una hoja {}".format(
        distribution_identifier, worksheets)
    assert len(worksheets) == 1, msg
    params["worksheet"] = worksheets[0]

    # coordenadas de los headers de las series
    params["headers_coord"] = list(df_fields.field_identifierCell)

    # fila donde empiezan los datos
    params["data_starts"] = map(row_from_cell_coord,
                                df_fields.field_dataStartCell)

    # frecuencia de las series
    field = get_field_metadata(catalog, distribution_identifier,
                               field_title="indice_tiempo")
    params["frequency"] = _convert_frequency(field["specialTypeDetail"])

    # coordenadas del header del indice de tiempo
    params["time_header_coord"] = df_distrib[
        df_distrib.field_title == "indice_tiempo"]["field_identifierCell"][0]

    # nombres de las series
    params["series_names"] = list(df_fields.field_title)

    return params


def scrape_dataframe(xl, worksheet, headers_coord, data_starts, frequency,
                     time_header_coord, series_names):
    params = deepcopy(XLSERIES_PARAMS)
    params["headers_coord"] = headers_coord
    params["data_starts"] = data_starts
    params["frequency"] = frequency
    params["time_header_coord"] = time_header_coord
    params["series_names"] = series_names

    return xl.get_data_frames(deepcopy(params), ws_name=worksheet)


def scrape_distribution(xl, etl_params, catalog, distribution_identifier):

    distribution_params = gen_distribution_params(
        etl_params, catalog, distribution_identifier)

    return scrape_dataframe(xl, **distribution_params)


def main(catalog_json_path, etl_params_path, ied_data_dir):
    pass


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
