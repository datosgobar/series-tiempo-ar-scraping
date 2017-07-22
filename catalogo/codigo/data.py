#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Métodos auxiliares para consultar y transformar series a partir de datasets
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
import pydatajson
from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers
from xlseries.strategies.clean.parse_time import TimeIsNotComposed
from xlseries import XlSeries
import json
from pydatajson.helpers import parse_repeating_time_interval as parse_freq

from helpers import row_from_cell_coord
import custom_exceptions as ce
from scrape_datasets import get_field_metadata, get_distribution_metadata

sys.path.insert(0, os.path.abspath(".."))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
LOGS_DIR = os.path.join(PROJECT_DIR, "catalogo", "logs")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
REPORTES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "reportes")
SERIES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "series")
DATASETS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "datasets")
DEFAULT_CATALOG_PATH = os.path.join(DATOS_DIR, "data.json")

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')


def get_time_series_data(
        field_ids,
        catalog_path=DEFAULT_CATALOG_PATH,
        export_path="/Users/abenassi/github/series-tiempo/catalogo/datos/dumps/tablero-ministerial-ied.csv", datasets_dir=DATASETS_DIR):

    if isinstance(field_ids, list):
        field_ids = {field_id: None for field_id in field_ids}

    catalog = readers.read_catalog(catalog_path)
    df = get_time_series_df(field_ids.keys(), use_id=True,
                            catalog=catalog)

    rows = []
    for field_id, data in df.to_dict().items():
        distribution_identifier = field_id.split("_")[0]
        field_metadata = get_field_metadata(catalog, distribution_identifier,
                                            field_id)
        distribution_metadata = get_distribution_metadata(
            catalog, distribution_identifier)

        time_index_freq = filter(
            lambda x: x.get("specialType") == "time_index",
            distribution_metadata["field"])[0]["specialTypeDetail"]

        for field_time, field_value in data.items():
            row = {
                "field_id": field_id,
                "field_time": field_time,
                "field_value": field_value,
                "field_title": field_metadata["title"],
                "field_units": field_metadata["units"],
                "field_description": field_metadata["description"],
                "field_short_description": field_ids.get(field_id),
                "distribution_time_index_freq": parse_freq(
                    time_index_freq, to="string")
            }
            rows.append(row)

    cols = [
        "field_id", "field_time", "field_value",
        "field_short_description", "field_units", "field_title",
        "field_description", "distribution_time_index_freq"
    ]
    df = pd.DataFrame(rows)[cols]

    if export_path:
        df.to_csv(export_path, encoding="utf8", index=False)

    return df


def get_time_series_df(field_ids, use_id=False, catalog=DEFAULT_CATALOG_PATH,
                       datasets_dir=DATASETS_DIR):
    catalog = readers.read_catalog(catalog)

    if not isinstance(field_ids, list):
        if isinstance(field_ids, dict):
            field_ids = field_ids.keys()
        else:
            field_ids = [field_ids]

    series_params = get_time_series_params(field_ids)
    assert len(series_params) > 0, "{} no están en la metadata".format(
        field_ids)

    # toma las series y genera un data frame concatenado
    if use_id:
        time_series = [
            get_time_series(*serie_params, field_id=field_id,
                            datasets_dir=datasets_dir)
            for field_id, serie_params in zip(field_ids, series_params)
        ]
    else:
        time_series = [
            get_time_series(*serie_params, datasets_dir=datasets_dir)
            for serie_params in series_params
        ]

    return pd.concat(time_series, axis=1)


def get_time_series_params(field_ids, catalog=DEFAULT_CATALOG_PATH):
    catalog = readers.read_catalog(catalog)

    # busca los ids de dataset y distribucion de la serie
    series_params = []
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if "field" in distribution:
                for field in distribution["field"]:
                    if field["id"] in field_ids:

                        series_params.append((
                            dataset["identifier"],
                            distribution["identifier"],
                            field["title"]
                        ))

    return series_params


def get_time_series_by_id(field_ids):

    if not isinstance(field_ids, list):
        field_ids = [field_ids]

    series_params = get_time_series_params(field_ids)


def get_time_series(dataset_id, distribution_id, field_title, fmt="df",
                    time_index="indice_tiempo", field_id=None,
                    datasets_dir=DATASETS_DIR):
    """Devuelve un dataframe con una serie de tiempo."""

    distribution_path = os.path.join(
        datasets_dir, dataset_id, distribution_id + ".csv")

    df = pd.read_csv(distribution_path, index_col=time_index,
                     parse_dates=True)[[field_title]]
    if field_id:
        df = df.rename(columns={field_title: field_id})

    if fmt == "df":
        return df

    elif fmt == "dict" or "list":
        series_dict = json.loads(
            df.to_json(orient="columns", date_format="iso")).values()[0]

        if fmt == "dict":
            return series_dict
        else:
            return sorted(series_dict.iteritems(), key=lambda x: x[0])

    else:
        raise Exception("No se reconoce el formato")


def get_time_series_dict(catalog, field_ids, datasets_dir):

    if not isinstance(field_ids, list):
        field_ids = [field_ids]

    series_params = get_time_series_params(field_ids)

    time_series = {
        field_id: {
            "metadata": get_field_metadata(catalog, field_id=field_id),
            "data": get_time_series(*serie_params, fmt="list",
                                    datasets_dir=datasets_dir)
        }
        for field_id, serie_params in zip(field_ids, series_params)
    }

    return time_series


def generate_time_series_jsons(ts_dict, jsons_dir=SERIES_DIR):

    for series_id, value in ts_dict.iteritems():
        file_name = "{}.json".format(series_id)

        with open(os.path.join(jsons_dir, file_name), "wb") as f:
            json.dump(value, f)
