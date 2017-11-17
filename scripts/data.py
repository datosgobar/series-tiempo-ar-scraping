#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Métodos auxiliares para consultar y transformar series a partir de datasets
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement

import os
import sys
import glob
import pandas as pd
import numpy as np
import arrow
import json
from unicodecsv import DictReader
from collections import OrderedDict
import pydatajson
import pydatajson.readers as readers
import pydatajson.writers as writers
from pydatajson.helpers import parse_repeating_time_interval as parse_freq

import custom_exceptions as ce
from paths import CATALOGS_DIR, CATALOG_PATH, SERIES_DIR, get_distribution_path
from paths import get_catalogs_path, get_catalog_path, get_catalog_ids

sys.path.insert(0, os.path.abspath(".."))

NOW = arrow.now().isoformat()
TODAY = arrow.now().format('YYYY-MM-DD')


def get_time_index_field(distribution):
    index_col = None
    if "field" in distribution:
        for field in distribution["field"]:
            if ("specialType" in field and
                    field["specialType"].strip().lower() == "time_index"):
                index_col = field["title"]
                break
    return index_col


def has_time_series(dataset_or_distribution):

    # es una distribución
    if "distribution" in dataset_or_distribution:
        dataset = dataset_or_distribution

        for distribution in dataset["distribution"]:
            if distribution_has_time_series(distribution):
                return True
        return False

    else:
        distribution = dataset_or_distribution
        return distribution_has_time_series(distribution)


def distribution_has_time_series(distribution):
    # analiza condiciones de una distribución para contener series de tiempo
    if "field" not in distribution:
        return False
    if not get_time_index_field(distribution):
        return False

    return True


def _get_theme_labels(catalog, dataset_theme):
    return ",".join([
        catalog.get_theme(theme_id).get("label", theme_id)
        for theme_id in dataset_theme
        if dataset_theme and catalog.get_theme(theme_id)
    ])


def generate_dump(dataset_ids=None, distribution_ids=None, series_ids=None,
                  catalogs_dir=CATALOGS_DIR, merged=False):

    rows_dump_values = []
    rows_dump_distrib_metadata = []

    # itera todas las distributiones disponibles en busca de series de tiempo
    for catalog_id in get_catalog_ids(catalogs_dir):
        catalog = get_catalog(catalog_id)

        time_series_datasets = []
        for dataset in catalog["dataset"]:
            if has_time_series(dataset):
                time_series_datasets.append(dataset)
        print("{} datasets para agregar del catalogo {}".format(
            len(time_series_datasets), catalog_id))

        for dataset in time_series_datasets:
            for distribution in dataset["distribution"]:

                # tiene que tener series documentadas
                if "field" not in distribution:
                    print("distribution {} no tiene `field`".format(
                        distribution["identifier"]
                    ))
                    continue

                # tiene que tener un campo marcado como "indice de tiempo"
                index_col = get_time_index_field(distribution)
                if not index_col:
                    print("Distribución {} no tiene indice de tiempo".format(
                        distribution["identifier"]
                    ))
                    continue

                # tiene que haber un archivo con datos disponible
                try:
                    distribution_path = get_distribution_path(
                        catalog_id,
                        dataset["identifier"],
                        distribution["identifier"],
                        catalogs_dir
                    )
                except:
                    print("No existe archivo para distribution {}".format(
                        distribution["identifier"]))
                    continue

                # la distribución está lista para ser agregada
                msg = "Agregando {} {} {} desde {}".format(
                    catalog_id, dataset["identifier"],
                    distribution["identifier"], distribution_path)
                # print(msg, end="\r" * len(msg))

                # hasheo metadata de field por field_title
                fields = {
                    field["title"]: field
                    for field in distribution["field"]
                }

                # genera filas del dump de metadatos
                row_dump = OrderedDict()

                row_dump["catalogo_id"] = catalog_id
                row_dump["dataset_id"] = dataset["identifier"]
                row_dump["distribucion_id"] = distribution["identifier"]
                row_dump["distribucion_titulo"] = distribution["title"]
                row_dump["distribucion_descripcion"] = distribution[
                    "description"]
                row_dump["distribucion_url_descarga"] = distribution[
                    "downloadURL"]
                row_dump["dataset_responsable"] = dataset["publisher"]["name"]
                row_dump["dataset_fuente"] = dataset["source"]
                row_dump["dataset_titulo"] = dataset["title"]
                row_dump["dataset_descripcion"] = dataset["description"]
                row_dump["dataset_tema"] = _get_theme_labels(
                    catalog, dataset.get("theme"))

                rows_dump_distrib_metadata.append(row_dump)

                # parsea una distribución en CSV a filas del dump
                with open(distribution_path, "r") as f:
                    r = DictReader(f, encoding="utf-8")

                    # sólo almacena desde el primer valor no nulo de la serie
                    # requiere que la distribución esté ordenada en el tiempo
                    started = {}
                    for row in r:
                        for field_title, value in row.iteritems():
                            time_index = field_title == index_col
                            exists_field = field_title in fields

                            if time_index or not exists_field:
                                continue

                            # no arranca hasta el primer valor no nulo
                            value = pd.to_numeric(value)
                            if started.get(field_title, pd.notnull(value)):
                                started[field_title] = True
                            else:
                                continue

                            row_dump = OrderedDict()

                            row_dump["catalogo_id"] = catalog_id
                            row_dump["dataset_id"] = dataset["identifier"]
                            row_dump["distribucion_id"] = distribution[
                                "identifier"]
                            row_dump["serie_id"] = fields[field_title]["id"]
                            row_dump["indice_tiempo"] = row[index_col]
                            row_dump["indice_tiempo_frecuencia"] = fields[
                                index_col]["specialTypeDetail"]
                            row_dump["valor"] = value
                            row_dump["serie_titulo"] = field_title
                            row_dump["serie_unidades"] = fields[
                                field_title].get("units")
                            row_dump["serie_descripcion"] = fields[
                                field_title]["description"]

                            rows_dump_values.append(row_dump)

    # genera un DataFrame conteniendo el dump
    df_values = pd.DataFrame(rows_dump_values)
    df_distrib_metadata = pd.DataFrame(rows_dump_distrib_metadata)

    # convierte los valores del índice de tiempo en tipo fecha
    df_values['indice_tiempo'] = df_values[
        'indice_tiempo'].astype('datetime64[ns]')

    # ordena por entidades del perfil de metadatos
    df_values_sorted = df_values.sort_values([
        "catalogo_id", "dataset_id", "distribucion_id", "serie_id",
        "indice_tiempo"], ascending=True)
    df_distrib_metadata_sorted = df_distrib_metadata.sort_values([
        "catalogo_id", "dataset_id", "distribucion_id"], ascending=True)

    if merged:
        return df_values_sorted.merge(
            df_distrib_metadata_sorted, how="left",
            on=["catalogo_id", "dataset_id", "distribucion_id"])
    else:
        return df_values_sorted, df_distrib_metadata_sorted


def get_catalog(catalog_id, catalogs_dir=CATALOGS_DIR):
    return pydatajson.DataJson(get_catalog_path(catalog_id))


def get_series_data(
        field_ids,
        export_path="/Users/abenassi/github/series-tiempo/catalogo/datos/dumps/tablero-ministerial-ied.csv",
        catalogs_dir=CATALOGS_DIR, logger=None
):

    if isinstance(field_ids, list):
        field_ids = {field_id: None for field_id in field_ids}

    # carga los catalogos necesarios
    catalog_ids = [
        field_params[0]
        for field_params in get_series_params(field_ids)
    ]

    rows = []
    for catalog_id, field_id in zip(catalog_ids, field_ids):
        catalog = get_catalog(catalog_id)

        if logger:
            logger.info(field_id)
        df = get_series_df(field_id, use_id=True, catalogs_dir=catalogs_dir)
        distribution_identifier = field_id.split("_")[0]
        dataset_identifier = distribution_identifier.split(".")[0]

        # tomo la metadata de cada entidad
        field_metadata = catalog.get_field(field_id)
        distribution_metadata = catalog.get_distribution(
            distribution_identifier)
        dataset_metadata = catalog.get_dataset(dataset_identifier)

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


def get_series_df(field_ids, use_id=False, catalogs_dir=CATALOGS_DIR):

    if not isinstance(field_ids, list):
        if isinstance(field_ids, dict):
            field_ids = field_ids.keys()
        else:
            field_ids = [field_ids]

    series_params = get_series_params(field_ids)
    assert len(series_params) > 0, "{} no están en la metadata".format(
        field_ids)

    # toma las series y genera un data frame concatenado
    if use_id:
        time_series = []
        for field_id, serie_params in zip(field_ids, series_params):
            try:
                time_series.append(
                    get_series(*serie_params, series_name=field_id,
                               catalogs_dir=catalogs_dir)
                )
            except Exception as e:
                print(e)
    else:
        time_series = []
        for serie_params in series_params:
            try:
                time_series.append(
                    get_series(*serie_params, catalogs_dir=catalogs_dir)
                )
            except Exception as e:
                print(e)

    return pd.concat(time_series, axis=1)


def get_series_params(field_ids, catalogs_dir=CATALOGS_DIR, catalogs=None):

    # carga los catálogos que encuentre, si no vienen cargados de antes
    if not catalogs:
        catalogs = load_catalogs()

    # busca los ids de las series en todos los catálogos
    series_params = {}
    for field_id in field_ids:
        for catalog_id, catalog in catalogs.iteritems():
            field_location = catalog.get_field_location(field_id)
            if field_location:
                series_params[field_id] = (
                    catalog_id,
                    field_location["dataset_identifier"],
                    field_location["distribution_identifier"],
                    field_location["field_title"]
                )
                break
        if not field_location:
            print(field_id, "no existe en ningun catalogo")

    return series_params


def get_series_by_id(
        field_id, fmt="df", time_index="indice_tiempo",
        catalogs_dir=CATALOGS_DIR, pct_change=None, series_name=None
):
    series_params = get_series_params([field_id], catalogs_dir=CATALOGS_DIR)

    return get_series(
        *series_params[field_id], fmt=fmt, time_index=time_index,
        catalogs_dir=catalogs_dir, pct_change=pct_change,
        series_name=series_name
    )


def get_series(
    catalog_id, dataset_id, distribution_id, field_title,
    fmt="df", time_index="indice_tiempo",
    catalogs_dir=CATALOGS_DIR, pct_change=None, series_name=None
):
    """Devuelve los datos de una serie de tiempo en diversos formatos."""

    distribution_path = get_distribution_path(
        catalog_id, dataset_id, distribution_id, catalogs_dir=catalogs_dir)

    serie = pd.read_csv(distribution_path, index_col=time_index,
                        parse_dates=True)[field_title]

    # modificaciones a la serie
    if pct_change:
        serie = serie.pct_change(pct_change)

    if series_name:
        serie = serie.rename(series_name)

    # formato
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


def load_catalogs(catalogs_dir=CATALOGS_DIR):
    catalogs = {}
    for catalog_id in get_catalog_ids(catalogs_dir):
        catalogs[catalog_id] = pydatajson.DataJson(
            get_catalog_path(catalog_id))
    return catalogs


def get_series_dict(field_ids, catalogs_dir, metadata_included=None,
                    debug_mode=False, catalogs=None):

    if not catalogs:
        catalogs = load_catalogs()

    if not field_ids:
        field_ids = []
        for dataset in catalog["dataset"]:
            for distribution in dataset["distribution"]:
                if "field" in distribution:
                    for field in distribution["field"]:
                        if (field.get("specialType") != "time_index" and
                                "id" in field):
                            field_ids.append(field["id"])

    series_params = get_series_params(field_ids, catalogs)

    time_series = {}
    for field_id, serie_params in series_params.items():
        try:
            time_series[field_id] = {
                "metadata": generate_api_metadata(
                    field_id, metadata_included=metadata_included,
                    catalogs=catalogs
                ),
                "data": get_series(
                    *serie_params, fmt="list",
                    catalogs_dir=catalogs_dir
                )
            }
        except Exception as e:
            print(field_id, repr(e))
            if debug_mode:
                raise
            else:
                continue

    return time_series


def generate_api_metadata(field_id, override_metadata=None,
                          metadata_included=None, catalogs=None):

    if not catalogs:
        catalogs = load_catalogs()

    # busca el dataset y distribution al que pertenece la serie
    # print(catalog.get_catalog_metadata(), field_id)
    for catalog in catalogs.values():
        field_location = catalog.get_field_location(field_id)
        if field_location:
            break

    if not field_location:
        msg = "Serie {} no existe en catalogo {}".format(
            field_id, catalog["title"])
        print(msg)
        return

    # print(field_location)

    # extrae la metadata de dataset, distribution y field
    field_meta = catalog.get_field(field_id)
    distrib_meta = catalog.get_distribution(
        field_location["distribution_identifier"])
    dataset_meta = catalog.get_dataset(field_location["dataset_identifier"])

    # copia todos los campos de metadata de dataset, distribution y field
    api_metadata = {}
    for key, value in field_meta.iteritems():
        field_name = "field_{}".format(key)
        if not metadata_included or field_name in metadata_included:
            api_metadata[field_name] = value
    for key, value in distrib_meta.iteritems():
        field_name = "distribution_{}".format(key)
        if not metadata_included or field_name in metadata_included:
            api_metadata[field_name] = value
    for key, value in dataset_meta.iteritems():
        field_name = "dataset_{}".format(key)
        if not metadata_included or field_name in metadata_included:
            api_metadata[field_name] = value

    # agrega frecuencia de la serie
    frequency = None
    for field in distrib_meta["field"]:
        if field["specialType"] == "time_index":
            frequency = field["specialTypeDetail"]
            break
    api_metadata["distribution_index_frequency"] = frequency

    # opcionalmente reemplaza campos de metadata por otros valores
    if override_metadata:
        api_metadata.update(override_metadata)

    return api_metadata


def generate_series_jsons(ts_dict, jsons_dir=SERIES_DIR):

    for series_id, value in ts_dict.iteritems():
        file_name = "{}.json".format(series_id)

        try:
            with open(os.path.join(jsons_dir, file_name), "wb") as f:
                json.dump(value, f, sort_keys=True)

        except Exception as e:
            print()
            print(series_id, e)
            print()
