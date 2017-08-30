#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scrapea datasets a partir de un catálogo y parámetros para XlSeries
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement

import os
import sys
import arrow
import pydatajson
import json
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pydatajson.helpers import parse_repeating_time_interval_to_days
import logging

from helpers import get_logger, freq_iso_to_pandas, compress_file, timeit
from data import get_series_data, generate_dump
from paths import CATALOG_PATH, DUMPS_PARAMS_PATH
from paths import CATALOGS_DIR, DUMPS_DIR, get_catalog_path

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
sys.path.insert(0, os.path.abspath(".."))

# campos extra que definen una observación.
OBSERVATIONS_COLS = ["distribucion_indice_tiempo", "valor"]

# ids que definen una serie.
SERIES_INDEX_COLS = ["catalog_id", "dataset_id", "distribucion_id", "serie_id"]


def _get_valor_anterior_anio(series_dataframe):
    series = pd.Series(list(series_dataframe.valor), list(
        series_dataframe.distribucion_indice_tiempo)).sort_index()
    return series.get(series.index[-1] - pd.DateOffset(years=1))


def generate_series_summary(df, series_index_cols=SERIES_INDEX_COLS,
                            observations_cols=OBSERVATIONS_COLS):
    """Genera y devuelve metadatos del dump a nivel de serie.

    Args:
        df (pandas.DataFrame): Dump completo de la base de series de tiempo.
        series_index_cols (list): Ids que definen una serie.
        observations_cols (list): Campos extra que definen una observación.

    Returns:
        pandas.DataFrame: Metadatos de series más indicadores calculados.
    """

    # toma las columnas relevantes para detalle a nivel de series
    series_cols = df.columns.drop(observations_cols)

    # agrupa por serie y elimina columnas innecesarias
    series_group = df.groupby(series_index_cols)
    df_series = df.drop(observations_cols, axis=1).drop_duplicates()
    df_series.set_index(series_index_cols, inplace=True)

    # CALCULA INDICADORES resumen sobre las series
    # rango temporal de la serie
    df_series["serie_indice_inicio"] = series_group[
        "distribucion_indice_tiempo"].min()
    df_series["serie_indice_final"] = series_group[
        "distribucion_indice_tiempo"].max()
    df_series["serie_valores_cant"] = series_group[
        "distribucion_indice_tiempo"].count()

    # estado de actualización de los datos
    # calcula días que pasaron por encima de período cubierto por datos
    df_series["serie_dias_no_cubiertos"] = df_series.apply(
        lambda x: (pd.datetime.now() - pd.to_datetime(x[
            "serie_indice_final"]).to_period(
            freq_iso_to_pandas(x[
                "distribucion_indice_frecuencia"],
                how="end")).to_timestamp(how="end")).days,
        axis=1)
    # si pasaron 2 períodos no cubiertos por datos, serie está desactualizada
    df_series["serie_actualizada"] = df_series.apply(
        lambda x: x["serie_dias_no_cubiertos"] < 2 *
        parse_repeating_time_interval_to_days(
            x["distribucion_indice_frecuencia"]),
        axis=1)

    # valores representativos nominales
    df_series["valor_ultimo"] = series_group.apply(
        lambda x: x.loc[x.distribucion_indice_tiempo.argmax(), "valor"])
    df_series["valor_anterior"] = series_group.apply(
        lambda x: pd.Series(list(x.valor), list(
            x.distribucion_indice_tiempo)).sort_index()[-2]
    )
    df_series["valor_anterior_anio"] = series_group.apply(
        _get_valor_anterior_anio)

    # valores representativos en variación porcentual
    df_series["var_pct_anterior"] = df_series[
        "valor_ultimo"] / df_series["valor_anterior"] - 1
    df_series["var_pct_anterior_anio"] = df_series[
        "valor_ultimo"] / df_series["valor_anterior_anio"] - 1

    return df_series.reset_index()


def save_to_csv(df, path):
    df.to_csv(path, encoding="utf-8", sep=str(","), index=False)


def save_to_xlsx(df, path, sheet_name="data"):
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df.to_excel(writer, sheet_name, merge_cells=False,
                encoding="utf-8", index=False)
    writer.save()


def save_to_dta(df, path, str_limit=244):
    df_stata = df.copy()
    for col in df_stata.columns:

        # limita el largo de los campos de texto
        if df_stata[col].dtype.name == "object":
            df_stata[col] = df_stata[col].str[:str_limit]

        # elimina los valores infinitos de los tipos decimales
        elif "float" in df_stata[col].dtype.name:
            df_stata[col] = df_stata[col].apply(
                lambda x: np.nan if np.isinf(x) else x)

    df_stata.to_stata(path, write_index=False)


def save_to_db(df, path):
    engine = create_engine('sqlite:///{}'.format(path), echo=True)
    df.to_sql("series_tiempo", engine, index=False, if_exists="replace")


DF_SAVE_METHODS = {
    "csv": save_to_csv,
    "xlsx": save_to_xlsx,
    "dta": save_to_dta,
    "db": save_to_db
}


# @timeit
def save_dump(df_dump, df_series, df_values,
              fmt="CSV", base_name="series-tiempo", base_dir=DUMPS_DIR):

    # crea paths a los archivos que va a generar
    base_path = os.path.join(base_dir, "{}".format(base_name))
    dump_path = "{}.{}".format(base_path, fmt.lower())
    dump_path_zip = "{}-{}.zip".format(base_path, fmt.lower())
    summary_path = "{}-resumen.{}".format(base_path, fmt.lower())
    values_path = "{}-observaciones.{}".format(base_path, fmt.lower())

    # elige el método para guardar el dump según el formato requerido
    save_method = DF_SAVE_METHODS[fmt.lower()]

    print()

    # guarda dump completo
    print("Guardando dump completo en {}...".format(fmt), end=" ")
    save_method(df_dump, dump_path)
    print("{}MB".format(os.path.getsize(dump_path) / 1000000))

    # guarda resumen de series
    print("Guardando resumen de series en {}...".format(fmt), end=" ")
    save_method(df_series, summary_path)
    print("{}MB".format(os.path.getsize(summary_path) / 1000000))

    # guarda dump mínimo de observaciones
    print("Guardando dump de observaciones en {}...".format(fmt), end=" ")
    save_method(df_values, values_path)
    print("{}MB".format(os.path.getsize(values_path) / 1000000))

    # guarda créditos

    # guarda dump completo comprimido
    print("Comprimiendo dump completo en {}...".format(fmt), end=" ")
    compress_file(dump_path, dump_path_zip)
    print("{}MB".format(os.path.getsize(dump_path_zip) / 1000000))


def main(catalogs_dir=CATALOGS_DIR, dumps_dir=DUMPS_DIR,
         series_index_cols=SERIES_INDEX_COLS,
         observations_cols=OBSERVATIONS_COLS):
    """Genera dumps completos de la base en distintos formatos"""
    logger = get_logger(__name__)

    # genera dump completo de la base de series
    print("Generando dump completo en DataFrame...", end=" ")
    df_dump = generate_dump(catalogs_dir=catalogs_dir)
    print("{} observaciones".format(len(df_dump)))

    # genera resumen descriptivo de series del dump
    print("Generando resumen de series en DataFrame...", end=" ")
    df_series = generate_series_summary(
        df_dump, series_index_cols, observations_cols)
    print("{} series".format(len(df_series)))

    # genera dump mínimo con las observaciones
    print("Generando dump mínimo de observaciones en DataFrame...", end=" ")
    df_values = df_dump[observations_cols + series_index_cols]
    print("{} observaciones".format(len(df_values)))

    # guarda los contenidos del dump en diversos formatos
    save_dump(df_dump, df_series, df_values, fmt="CSV", base_dir=dumps_dir)
    save_dump(df_dump, df_series, df_values, fmt="XLSX", base_dir=dumps_dir)
    save_dump(df_dump, df_series, df_values, fmt="DTA", base_dir=dumps_dir)
    save_dump(df_dump, df_series, df_values, fmt="DB", base_dir=dumps_dir)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
