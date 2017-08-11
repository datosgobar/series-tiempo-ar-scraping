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
from unicodecsv import DictReader
from collections import OrderedDict

from helpers import row_from_cell_coord
import custom_exceptions as ce
from scrape_datasets import get_field_metadata, get_distribution_metadata
from scrape_datasets import get_dataset_metadata
from scrape_datasets import find_distribution_identifier
from paths import DATASETS_DIR, DEFAULT_CATALOG_PATH, SERIES_DIR

sys.path.insert(0, os.path.abspath(".."))

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')


def generate_dump(dataset_ids=None, distribution_ids=None, series_ids=None,
                  datasets_dir=DATASETS_DIR, catalog=DEFAULT_CATALOG_PATH,
                  index_col="indice_tiempo"):
    catalog = readers.read_catalog(catalog)

    rows_dump = []
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if "field" not in distribution:
                continue

            distribution_path = os.path.join(
                DATASETS_DIR, unicode(dataset["identifier"]),
                "{}.csv".format(unicode(distribution["identifier"]))
            )

            if not os.path.exists(distribution_path):
                continue

            # hasheo metadata de field por field_title
            fields = {
                field["title"]: field
                for field in distribution["field"]
            }

            # parsea CSV a filas del dump
            with open(distribution_path, "r") as f:
                r = DictReader(f, encoding="utf-8-sig")

                for row in r:
                    for field_title, value in row.iteritems():
                        time_index = field_title == index_col
                        exists_field = field_title in fields

                        if time_index or not exists_field:
                            continue

                        row_dump = OrderedDict()

                        row_dump["dataset_id"] = dataset["identifier"]
                        row_dump["distribucion_id"] = distribution[
                            "identifier"]
                        row_dump["serie_id"] = fields[field_title]["id"]
                        row_dump["indice_tiempo"] = row[index_col]
                        row_dump["indice_frecuencia"] = fields[
                            index_col]["specialTypeDetail"]
                        row_dump["valor"] = value
                        row_dump["serie_titulo"] = field_title
                        row_dump["serie_unidades"] = fields[
                            field_title].get("units")
                        row_dump["serie_unidades"] = fields[
                            field_title].get("units")
                        row_dump["serie_descripcion"] = fields[
                            field_title]["description"]
                        row_dump["dataset_titulo"] = dataset["title"]
                        row_dump["dataset_descripcion"] = dataset[
                            "description"]
                        row_dump["dataset_responsable"] = dataset["source"]
                        row_dump["dataset_fuente"] = dataset["source"]
                        row_dump["distribucion_titulo"] = distribution["title"]
                        row_dump["distribucion_descripcion"] = distribution[
                            "description"]

                        rows_dump.append(row_dump)

    df = pd.DataFrame(rows_dump)

    return df


def get_time_series_data(
        field_ids,
        catalog_path=DEFAULT_CATALOG_PATH,
        export_path="/Users/abenassi/github/series-tiempo/catalogo/datos/dumps/tablero-ministerial-ied.csv",
        datasets_dir=DATASETS_DIR, logger=None
):

    if isinstance(field_ids, list):
        field_ids = {field_id: None for field_id in field_ids}

    catalog = readers.read_catalog(catalog_path)

    rows = []
    for field_id in field_ids:
        if logger:
            logger.info(field_id)
        df = get_time_series_df(field_id, use_id=True, catalog=catalog)
        distribution_identifier = field_id.split("_")[0]
        dataset_identifier = distribution_identifier.split(".")[0]

        # tomo la metadata de cada entidad
        field_metadata = get_field_metadata(
            catalog, distribution_identifier, field_id
        )
        distribution_metadata = get_distribution_metadata(
            catalog, distribution_identifier)
        dataset_metadata = get_dataset_metadata(catalog, dataset_identifier)

        time_index_freq = filter(
            lambda x: x.get("specialType") == "time_index",
            distribution_metadata["field"])[0]["specialTypeDetail"]

        for field_time, field_value in df[field_id].to_dict().iteritems():
            row = {
                "dataset_identifier": dataset_metadata["identifier"],
                "dataset_title": dataset_metadata["title"],
                "dataset_theme": dataset_metadata["theme"],
                "dataset_source": dataset_metadata["source"],
                "dataset_contactPoint_fn": dataset_metadata["contactPoint"]["fn"],
                "distribution_identifier": distribution_identifier,
                "distribution_title": distribution_metadata["title"],
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
        "dataset_identifier", "dataset_title",
        "dataset_theme", "dataset_source", "dataset_contactPoint_fn",
        "distribution_identifier", "distribution_title",
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
    series_params = {}
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            if "field" in distribution:
                for field in distribution["field"]:
                    if field["id"] in field_ids:
                        series_params[field["id"]] = (
                            dataset["identifier"],
                            distribution["identifier"],
                            field["title"]
                        )

    return [series_params[field_id] for field_id in field_ids]


def get_time_series_by_id(field_id, fmt="df"):
    series_params = get_time_series_params([field_id])
    return get_time_series(*series_params[0], fmt=fmt)


def get_time_series(dataset_id, distribution_id, field_title, fmt="df",
                    time_index="indice_tiempo", field_id=None,
                    datasets_dir=DATASETS_DIR, pct_change=None):
    """Devuelve un dataframe con una serie de tiempo."""

    distribution_path = os.path.join(
        datasets_dir, dataset_id, distribution_id + ".csv")

    serie = pd.read_csv(distribution_path, index_col=time_index,
                        parse_dates=True)[field_title]
    if pct_change:
        serie = serie.pct_change(pct_change)

    if field_id:
        serie = serie.rename(field_id)

    if fmt == "df":
        return pd.DataFrame(serie)

    elif fmt == "serie":
        return serie

    elif fmt == "dict" or "list":
        series_dict = json.loads(
            pd.DataFrame(serie).to_json(orient="columns", date_format="iso")
        ).values()[0]

        if fmt == "dict":
            return series_dict
        else:
            return sorted(series_dict.iteritems(), key=lambda x: x[0])

    else:
        raise Exception("No se reconoce el formato")


def get_time_series_dict(catalog, field_params, datasets_dir):

    field_ids = field_params.keys()
    series_params = get_time_series_params(field_ids)

    time_series = {}
    for field_id, serie_params in zip(field_ids, series_params):
        time_series[field_id] = {
            "metadata": generate_api_metadata(
                catalog, field_id, field_params[field_id]),
            "data": get_time_series(
                *serie_params, fmt="list",
                datasets_dir=datasets_dir,
                pct_change=field_params[field_id].get("pct_change"))
        }

    return time_series


def generate_api_metadata(catalog, field_id, override_metadata=None):
    field_meta = get_field_metadata(catalog, field_id=field_id)
    distribution_identifier = find_distribution_identifier(catalog, field_id)
    dataset_identifier = distribution_identifier.split(".")[0]

    distrib_meta = get_distribution_metadata(catalog, distribution_identifier,
                                             dataset_identifier)

    frequency = None
    for field in distrib_meta["field"]:
        if field["specialType"] == "time_index":
            frequency = field["specialTypeDetail"]
            break

    api_metadata = {
        "title": field_meta["title"],
        "frequency": frequency,
        "units": field_meta["units"],
        "type": field_meta["type"],
        "id": field_meta["id"],
        "description": field_meta["description"]
    }
    api_metadata.update(override_metadata)

    return api_metadata


def generate_time_series_jsons(ts_dict, jsons_dir=SERIES_DIR):

    for series_id, value in ts_dict.iteritems():
        file_name = "{}.json".format(series_id)

        with open(os.path.join(jsons_dir, file_name), "wb") as f:
            json.dump(value, f)
