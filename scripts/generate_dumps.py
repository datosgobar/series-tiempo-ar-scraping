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
from unidecode import unidecode

from helpers import get_logger, freq_iso_to_pandas, compress_file, timeit
from helpers import indicators_to_text, FREQ_ISO_TO_HUMAN, safe_sheet_name
from data import get_series_data, generate_dump
from paths import CATALOG_PATH, DUMPS_PARAMS_PATH, REPORTES_DIR
from paths import CATALOGS_DIR, DUMPS_DIR, get_catalog_path

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
sys.path.insert(0, os.path.abspath(".."))

# campos extra que definen una observación.
OBSERVATIONS_COLS = ["indice_tiempo", "valor"]

# metadata necesaria para usar los valores
CRITICAL_METADATA_COLS = ["indice_tiempo_frecuencia"]

# ids que definen una serie.
SERIES_INDEX_COLS = ["catalog_id", "dataset_id", "distribution_id", "serie_id"]


def _is_series_updated(row):
    index_freq = row["indice_tiempo_frecuencia"]
    period_days = parse_repeating_time_interval_to_days(index_freq)
    periods_tolerance = {
        "R/P1Y": 2,
        "R/P6M": 2,
        "R/P3M": 2,
        "R/P1M": 3,
        "R/P1D": 14
    }
    days_tolerance = periods_tolerance.get(index_freq, 2) * period_days
    return row["serie_dias_no_cubiertos"] < days_tolerance


def _get_serie_valor_anterior_anio(series_dataframe):
    series = pd.Series(list(series_dataframe.valor), list(
        series_dataframe.indice_tiempo)).sort_index()
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
    df_series["serie_indice_inicio"] = series_group["indice_tiempo"].min()
    df_series["serie_indice_final"] = series_group["indice_tiempo"].max()
    df_series["serie_valores_cant"] = series_group["indice_tiempo"].count()

    # estado de actualización de los datos
    # calcula días que pasaron por encima de período cubierto por datos
    df_series["serie_dias_no_cubiertos"] = df_series.apply(
        lambda x: (pd.datetime.now() - pd.to_datetime(x[
            "serie_indice_final"]).to_period(
            freq_iso_to_pandas(x[
                "indice_tiempo_frecuencia"],
                how="end")).to_timestamp(how="end")).days,
        axis=1)
    # si pasaron 2 períodos no cubiertos por datos, serie está desactualizada
    df_series["serie_actualizada"] = df_series.apply(
        _is_series_updated, axis=1)

    # valores representativos nominales
    df_series["serie_valor_ultimo"] = series_group.apply(
        lambda x: x.loc[x.indice_tiempo.argmax(), "valor"])
    df_series["serie_valor_anterior"] = series_group.apply(
        lambda x: pd.Series(list(x.valor), list(
            x.indice_tiempo)).sort_index()[-2]
    )
    # df_series["serie_valor_anterior_anio"] = series_group.apply(
    #     _get_serie_valor_anterior_anio)

    # valores representativos en variación porcentual
    df_series["serie_var_pct_anterior"] = df_series[
        "serie_valor_ultimo"] / df_series["serie_valor_anterior"] - 1
    # df_series["serie_var_pct_anterior_anio"] = df_series[
    #     "serie_valor_ultimo"] / df_series["serie_valor_anterior_anio"] - 1

    # controlo tipos
    df_series["serie_valores_cant"] = df_series[
        "serie_valores_cant"].astype(int)
    df_series["serie_dias_no_cubiertos"] = df_series[
        "serie_dias_no_cubiertos"].astype(int)
    df_series["serie_valor_ultimo"] = df_series[
        "serie_valor_ultimo"].astype(float)
    df_series["serie_valor_anterior"] = df_series[
        "serie_valor_anterior"].astype(float)
    # df_series["serie_valor_anterior_anio"] = df_series[
    #     "serie_valor_anterior_anio"].astype(float)
    df_series["serie_var_pct_anterior"] = df_series[
        "serie_var_pct_anterior"].astype(float)
    # df_series["serie_var_pct_anterior_anio"] = df_series[
    #     "serie_var_pct_anterior_anio"].astype(float)

    return df_series.reset_index()


def generate_fuentes_summary(df_series):
    group_fuentes = df_series.groupby("dataset_fuente")

    # calcula indicadores de las fuentes
    df_fuentes_valores = group_fuentes.sum()[["serie_valores_cant"]].rename(
        columns={"serie_valores_cant": "valores_cant"})
    df_fuentes_series = group_fuentes.count()[["serie_titulo"]].rename(
        columns={"serie_titulo": "series_cant"})
    df_fuentes_primero = group_fuentes.min()[["serie_indice_inicio"]].rename(
        columns={"serie_indice_inicio": "fecha_primer_valor"})
    df_fuentes_ultimo = group_fuentes.max()[["serie_indice_final"]].rename(
        columns={"serie_indice_final": "fecha_ultimo_valor"})
    fuentes_indics = [df_fuentes_series, df_fuentes_valores,
                      df_fuentes_primero, df_fuentes_ultimo]

    df_fuentes = pd.concat(fuentes_indics, axis=1).sort_values(
        "series_cant", ascending=False)

    return df_fuentes.reset_index()


def save_to_csv(df, path):
    df.to_csv(path, encoding="utf-8", sep=str(","), index=False)


def save_to_xlsx(df, path, sheet_name="data", split_field=None,
                 split_values_map=None):
    split_values_map = split_values_map or {}
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    if split_field:
        split_values = df[split_field].unique()
        for split_value in split_values:
            sheet_name = safe_sheet_name(
                split_values_map.get(split_value, split_value))

            df[df[split_field] == split_value].to_excel(
                writer, sheet_name,
                merge_cells=False, encoding="utf-8", index=False)
    else:
        df.to_excel(writer, sheet_name, merge_cells=False,
                    encoding="utf-8", index=False)
    writer.save()


def save_to_dta(df, path, str_limit=244):
    df_stata = df.copy()
    for col in df_stata.columns:

        # limita el largo de los campos de texto
        if df_stata[col].dtype.name == "object":
            df_stata[col] = df_stata[col].astype(unicode).str[:str_limit]

        # elimina los valores infinitos de los tipos decimales
        elif "float" in df_stata[col].dtype.name:
            df_stata[col] = df_stata[col].apply(
                lambda x: np.nan if np.isinf(x) else x)

    df_stata.to_stata(path, write_index=False)


def save_to_db(df, path):
    df_sqlite = df.copy()
    engine = create_engine('sqlite:///{}'.format(path), echo=True)

    # cambio el formato de tiempo a string, si hay un índice de tiempo
    if "indice_tiempo" in df_sqlite.columns:
        df_sqlite['indice_tiempo'] = df_sqlite[
            'indice_tiempo'].dt.strftime("%Y-%m-%d")

    df_sqlite.to_sql("series_tiempo", engine, index=False, if_exists="replace")


DF_SAVE_METHODS = {
    "csv": save_to_csv,
    "xlsx": save_to_xlsx,
    "dta": save_to_dta,
    "db": save_to_db
}


# @timeit
def save_dump(df_dump, df_series, df_values, df_fuentes,
              fmt="CSV", base_name="series-tiempo", base_dir=DUMPS_DIR):
    logger = get_logger(__name__)

    # crea paths a los archivos que va a generar
    base_path = os.path.join(base_dir, "{}".format(base_name))
    dump_path = "{}.{}".format(base_path, fmt.lower())
    dump_path_zip = "{}-{}.zip".format(base_path, fmt.lower())
    summary_path = "{}-metadatos.{}".format(base_path, fmt.lower())
    values_path = "{}-valores.{}".format(base_path, fmt.lower())
    sources_path = "{}-fuentes.{}".format(base_path, fmt.lower())

    # elige el método para guardar el dump según el formato requerido
    save_method = DF_SAVE_METHODS[fmt.lower()]

    print()

    # guarda dump completo
    logger.info("Guardando dump completo en {}...".format(fmt))
    sys.stdout.flush()
    if fmt.lower() == "xlsx":
        save_method(
            df_dump, dump_path, split_field="indice_tiempo_frecuencia",
            split_values_map=FREQ_ISO_TO_HUMAN)
    else:
        save_method(df_dump, dump_path)
    logger.info("{}MB".format(os.path.getsize(dump_path) / 1000000))

    # guarda resumen de series
    logger.info("Guardando resumen de series en {}...".format(fmt))
    sys.stdout.flush()
    save_method(df_series, summary_path)
    logger.info("{}MB".format(os.path.getsize(summary_path) / 1000000))

    # guarda resumen de fuentes
    logger.info("Guardando resumen de fuentes en {}...".format(fmt))
    sys.stdout.flush()
    save_method(df_fuentes, sources_path)
    logger.info("{}MB".format(os.path.getsize(sources_path) / 1000000))

    # guarda dump mínimo de valores
    logger.info("Guardando dump de valores en {}...".format(fmt))
    sys.stdout.flush()
    if fmt.lower() == "xlsx":
        save_method(
            df_values, values_path, split_field="indice_tiempo_frecuencia",
            split_values_map=FREQ_ISO_TO_HUMAN)
    else:
        save_method(df_values, values_path)
    logger.info("{}MB".format(os.path.getsize(values_path) / 1000000))

    # guarda dump completo comprimido
    logger.info("Comprimiendo dump completo en {}...".format(fmt))
    compress_file(dump_path, dump_path_zip)
    logger.info("{}MB".format(os.path.getsize(dump_path_zip) / 1000000))


# selección reducida de columnas para los dumps que son demasiado pesados
COMPLETE_DUMP_COLS = [
    "catalog_id",
    "dataset_id",
    "distribution_id",
    "serie_id",
    "indice_tiempo",
    "indice_tiempo_frecuencia",
    "valor",
    "serie_titulo",
    "serie_unidades",
    "serie_descripcion",
    "distribution_titulo",
    "distribution_descripcion",
    # "distribution_downloadURL",
    "dataset_responsable",
    "dataset_fuente",
    "dataset_titulo",
    "dataset_descripcion"
]

STATA_DUMP_COLS = [
    "catalog_id",
    "dataset_id",
    "distribution_id",
    "serie_id",
    "indice_tiempo",
    "indice_tiempo_frecuencia",
    "valor",
    "serie_titulo",
    "serie_unidades",
    "serie_descripcion",
    "distribution_titulo",
    # "distribution_descripcion",
    # "distribution_downloadURL",
    "dataset_responsable",
    "dataset_fuente",
    "dataset_titulo"
    # "dataset_descripcion"
]


def main(catalogs_dir=CATALOGS_DIR, dumps_dir=DUMPS_DIR,
         series_index_cols=SERIES_INDEX_COLS,
         observations_cols=OBSERVATIONS_COLS,
         critical_metadata_cols=CRITICAL_METADATA_COLS,
         formats=None):
    """Genera dumps completos de la base en distintos formatos"""
    logger = get_logger(__name__)

    # genera dump completo de la base de series
    logger.info("Generando dump completo en DataFrame...")
    df_dump = generate_dump(catalogs_dir=catalogs_dir, merged=True)
    logger.info("{} valores".format(len(df_dump)))

    # genera resumen descriptivo de series del dump
    logger.info("Generando resumen de series en DataFrame...")
    df_series = generate_series_summary(
        df_dump, series_index_cols, observations_cols)
    logger.info("{} series".format(len(df_series)))

    # genera resumen descriptivo de series del dump
    logger.info("Generando resumen de fuentes en DataFrame...")
    df_fuentes = generate_fuentes_summary(df_series)
    logger.info("{} fuentes".format(len(df_fuentes)))

    # genera dump mínimo con las valores
    logger.info("Generando dump mínimo de valores en DataFrame...")
    df_values = df_dump[series_index_cols +
                        observations_cols + critical_metadata_cols]
    logger.info("{} valores".format(len(df_values)))

    # guarda los contenidos del dump en diversos formatos
    if not formats or "csv" in formats:
        save_dump(df_dump[COMPLETE_DUMP_COLS],
                  df_series, df_values, df_fuentes,
                  fmt="CSV", base_dir=dumps_dir)
    if not formats or "xlsx" in formats:
        save_dump(df_dump[COMPLETE_DUMP_COLS],
                  df_series, df_values, df_fuentes,
                  fmt="XLSX", base_dir=dumps_dir)
    # menos columnas para STATA porque el formato es muy pesado
    if not formats or "dta" in formats:
        save_dump(df_dump[STATA_DUMP_COLS],
                  df_series, df_values, df_fuentes,
                  fmt="DTA", base_dir=dumps_dir)
    # desacivo logging para SQLITE porque es muy verborrágico
    if not formats or "db" in formats:
        logger.disabled = True
        save_dump(df_dump[COMPLETE_DUMP_COLS],
                  df_series, df_values, df_fuentes,
                  fmt="DB", base_dir=dumps_dir)
        logger.disabled = False

    # calcula indicadores sumarios del dump
    indicators = {
        "Dump - catalogos": len(df_series.catalog_id.unique()),
        "Dump - datasets": len(
            df_series[['catalog_id', 'dataset_id']].drop_duplicates()),
        "Dump - distribuciones": len(df_series[[
            'catalog_id', 'dataset_id', 'distribution_id']].drop_duplicates()),
        "Dump - series": len(df_series),
        "Dump - valores": len(df_values),
        "Dump - responsables": len(df_series.dataset_responsable.unique()),
        "Dump - fuentes": len(df_fuentes)
    }
    message = indicators_to_text(indicators)
    with open(os.path.join(REPORTES_DIR, "mail_message.txt"), "a") as f:
        f.write(message)
    print(message)


if __name__ == '__main__':
    if len(sys.argv) > 3:
        formats = sys.argv[3].split(",")
    else:
        formats = None
    main(sys.argv[1], sys.argv[2], formats=formats)
