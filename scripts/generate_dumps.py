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

from helpers import get_logger, freq_iso_to_pandas, compress_file, timeit
from data import get_series_data, generate_dump
from paths import CATALOG_PATH, DUMPS_PARAMS_PATH
from paths import CATALOGS_DIR, DUMPS_DIR, get_catalog_path

sys.path.insert(0, os.path.abspath(".."))


def _get_valor_anterior_anio(series_dataframe):
    series = pd.Series(list(series_dataframe.valor), list(
        series_dataframe.distribucion_indice_tiempo)).sort_index()
    return series.get(series.index[-1] - pd.DateOffset(years=1))


def generate_series_summary(df):
    drop_cols = ["distribucion_indice_tiempo", "valor"]
    index_cols = ["dataset_id", "distribucion_id", "serie_id"]
    series_cols = df.columns.drop(drop_cols)
    series_group = df.groupby(index_cols)

    df_series = df.drop(
        drop_cols, axis=1).drop_duplicates().set_index(index_cols)

    # indicadores resumen sobre las series
    # rango temporal de la serie
    df_series["serie_indice_inicio"] = series_group[
        "distribucion_indice_tiempo"].min()
    df_series["serie_indice_final"] = series_group[
        "distribucion_indice_tiempo"].max()
    df_series["serie_valores_cant"] = series_group[
        "distribucion_indice_tiempo"].count()

    # estado de actualización de los datos
    df_series["serie_dias_no_cubiertos"] = df_series.apply(
        lambda x: (pd.datetime.now() - pd.to_datetime(x["serie_indice_final"]).to_period(
            freq_iso_to_pandas("R/P3M", how="end")).to_timestamp(how="end")).days,
        axis=1)
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
def save_dump(df_dump, df_series,
              fmt="CSV", base_name="series-tiempo", base_dir=DUMPS_DIR):

    # crea paths a los archivos que va a generar
    base_path = os.path.join(base_dir, "{}".format(base_name))
    dump_path = "{}.{}".format(base_path, fmt.lower())
    dump_path_zip = "{}-{}.zip".format(base_path, fmt.lower())
    resumen_path = "{}-resumen.{}".format(base_path, fmt.lower())

    # elige el método para guardar el dump según el formato requerido
    save_method = DF_SAVE_METHODS[fmt.lower()]

    print()

    # crea dump completo
    print("Guardando dump completo en {}...".format(fmt), end=" ")
    save_method(df_dump, dump_path)
    print("{}MB".format(os.path.getsize(dump_path) / 1000000))

    # crea dump desnormalizado

    # crea resumen de series
    print("Guardando resumen de series en {}...".format(fmt), end=" ")
    save_method(df_series, resumen_path)
    print("{}MB".format(os.path.getsize(resumen_path) / 1000000))

    # crea créditos

    # crea dump completo comprimido
    print("Comprimiendo dump en {}...".format(fmt), end=" ")
    compress_file(dump_path, dump_path_zip)
    print("{}MB".format(os.path.getsize(dump_path_zip) / 1000000))


def main(catalogs_dir=CATALOGS_DIR, dumps_dir=DUMPS_DIR):
    # genera dumps completos de la base en distintos formatos
    logger = get_logger(__name__)

    # genera dump completo de la base de series
    print("Generando dump completo en DataFrame.")
    df_dump = generate_dump(catalogs_dir=catalogs_dir)

    # genera resumen descriptivo de series del dump
    print("Generando resumen de series en DataFrame.")
    df_series = generate_series_summary(df_dump)

    # guarda los contenidos del dump en diversos formatos
    save_dump(df_dump, df_series, "CSV")
    save_dump(df_dump, df_series, "XLSX")
    save_dump(df_dump, df_series, "DTA")
    save_dump(df_dump, df_series, "DB")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
