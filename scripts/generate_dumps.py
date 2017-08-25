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
import zipfile
import datetime
import pandas as pd
from sqlalchemy import create_engine

from helpers import get_logger
from data import get_series_data, generate_dump
from paths import CATALOG_PATH, DUMPS_PARAMS_PATH
from paths import CATALOGS_DIR, DUMPS_DIR, get_catalog_path

sys.path.insert(0, os.path.abspath(".."))


# TODO: agregar al dump
# theme_label
# dataset_title
# distribution_title
# dataset_identifier

def print_info(archive_name):
    zf = zipfile.ZipFile(archive_name)
    for info in zf.infolist():
        print(info.filename)
        print('\tComment:\t', info.comment)
        print('\tModified:\t', datetime.datetime(*info.date_time))
        print('\tSystem:\t\t', info.create_system, '(0 = Windows, 3 = Unix)')
        print('\tZIP version:\t', info.create_version)
        print('\tCompressed:\t', info.compress_size, 'bytes')
        print('\tUncompressed:\t', info.file_size, 'bytes')


def compress_file(from_path, to_path):
    print("Comprimiendo {} en {}".format(from_path, to_path))
    zf = zipfile.ZipFile(to_path, 'w', zipfile.ZIP_DEFLATED)
    try:
        zf.write(from_path)
    finally:
        zf.close()

    print_info(to_path)


def generate_series_summary(df):
    drop_cols = ["distribucion_indice_tiempo", "valor"]
    index_cols = ["dataset_id", "distribucion_id", "serie_id"]
    series_cols = df.columns.drop(drop_cols)
    series_group = df.groupby(index_cols)

    df_series = df.drop(
        drop_cols, axis=1).drop_duplicates().set_index(index_cols)

    # indicadores resumen sobre las series
    df_series["distribucion_indice_inicio"] = series_group[
        "distribucion_indice_tiempo"].min()
    df_series["distribucion_indice_final"] = series_group[
        "distribucion_indice_tiempo"].max()
    df_series["valor_ultimo"] = series_group.apply(
        lambda x: x.loc[x.distribucion_indice_tiempo.argmax(), "valor"])

    return df_series


def main(catalogs_dir=CATALOGS_DIR, dumps_dir=DUMPS_DIR):
    logger = get_logger(__name__)

    # genera dumps completos de la base en distintos formatos
    dump_path = os.path.join(DUMPS_DIR, "series-tiempo.{}")
    print("Generando dump completo en DataFrame.")
    df = generate_dump(catalogs_dir=catalogs_dir)
    df_series = generate_series_summary(df)
    resumen_path = os.path.join(
        os.path.dirname(dump_path), "series-tiempo-resumen.xlsx")
    df_series.to_excel(resumen_path, "resumen",
                       index=True, index_label=True, merge_cells=False)

    # CSV
    print("Generando dump completo en CSV.")
    path = dump_path.format("csv")
    df.to_csv(path, encoding="utf-8", sep=str(","), index=False)
    print("{}MB".format(os.path.getsize(path) / 1000000))
    zip_path = os.path.join(os.path.dirname(path), "series-tiempo-csv.zip")
    compress_file(path, zip_path)
    print("{}MB".format(os.path.getsize(zip_path) / 1000000))

    # EXCEL
    print("Generando dump completo en XLSX.")
    path = dump_path.format("xlsx")
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df.to_excel(writer, "data", merge_cells=False,
                encoding="utf-8", index=False)
    writer.save()
    print("{}MB".format(os.path.getsize(path) / 1000000))
    zip_path = os.path.join(os.path.dirname(path), "series-tiempo-xlsx.zip")
    compress_file(path, zip_path)
    print("{}MB".format(os.path.getsize(zip_path) / 1000000))

    # SQLITE
    print("Generando dump completo en SQLITE.")
    path = dump_path.format("db")
    engine = create_engine('sqlite:///{}'.format(path), echo=True)
    df.to_sql("series_tiempo", engine, index=False, if_exists="replace")
    print("{}MB".format(os.path.getsize(path) / 1000000))
    zip_path = os.path.join(os.path.dirname(path), "series-tiempo-db.zip")
    compress_file(path, zip_path)
    print("{}MB".format(os.path.getsize(zip_path) / 1000000))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
