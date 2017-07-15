#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Genera un CSV con los parámetros necesarios para parsear excels de IED
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys

import pandas as pd

from helpers import find_ws_name

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
REPORTES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "reportes")
PARAMS_FIELDS = [
    "distribution_iedFileURL", "distribution_iedFileSheet",
    "distribution_identifier", "distribution_title", "field_title", "field_id",
    "field_identifierCell", "field_dataStartCell"
]


def get_params_from_sheet(catalog_xlsx_path,
                          params_sheet_name="Parametros ETL"):
    return pd.read_excel(catalog_xlsx_path, params_sheet_name)


def get_params_from_model(catalog_xlsx_path,
                          distribution_sheet_name="distribution",
                          field_sheet_name="field"):
    distribution_cols = [
        "distribution_iedFileURL", "distribution_iedFileSheet",
        "distribution_identifier", "distribution_title", "dataset_identifier",
        "dataset_title"
    ]
    field_cols = [
        "field_title", "field_id", "field_identifierCell",
        "field_dataStartCell", "dataset_identifier", "dataset_title",
        "distribution_identifier", "distribution_title"
    ]

    df_distribution = pd.read_excel(catalog_xlsx_path, find_ws_name(
        catalog_xlsx_path, "distribution"))[distribution_cols]
    df_field = pd.read_excel(catalog_xlsx_path, find_ws_name(
        catalog_xlsx_path, "field"))[field_cols]

    merged = df_field[field_cols].merge(
        df_distribution[distribution_cols], how="outer", indicator=True,
        on=["dataset_identifier", "dataset_title",
            "distribution_identifier", "distribution_title"]
    )

    # genera reportes
    merged[merged['_merge'] == 'left_only'][PARAMS_FIELDS].to_excel(
        os.path.join(REPORTES_DIR, "reporte-fields-sin-distribution.xlsx"),
        encoding="utf-8", index=False)
    merged[merged['_merge'] == 'right_only'][PARAMS_FIELDS].to_excel(
        os.path.join(REPORTES_DIR, "reporte-distribution-sin-fields.xlsx"),
        encoding="utf-8", index=False)

    df_etl_params = merged[merged['_merge'] == 'both']

    return df_etl_params


def main(catalog_xlsx_path, excels_urls_path):

    df_etl_params = get_params_from_model(catalog_xlsx_path)
    df_etl_params[PARAMS_FIELDS].to_csv(
        "catalogo/datos/etl_params.csv",
        encoding="utf8", index=False
    )


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
