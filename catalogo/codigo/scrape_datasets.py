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
import pydatajson
from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers
from xlseries.strategies.clean.parse_time import TimeIsNotComposed
from xlseries import XlSeries

from helpers import row_from_cell_coord
import custom_exceptions as ce

sys.path.insert(0, os.path.abspath(".."))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
LOGS_DIR = os.path.join(PROJECT_DIR, "catalogo", "logs")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
DATASETS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "datasets")
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
    # 'time_composed': True
}


def get_dataset_metadata(catalog, dataset_identifier):
    datasets = filter(lambda x: x.get("identifier") == dataset_identifier,
                      catalog["dataset"])

    if len(datasets) > 1:
        raise ce.DatasetIdRepetitionError(dataset_identifier, datasets)
    elif len(datasets) == 0:
        return None
    else:
        return datasets[0]


def get_distribution_metadata(catalog, distribution_identifier,
                              dataset_identifier=None):

    dataset_identifier = (dataset_identifier or
                          distribution_identifier.split(".")[0])
    dataset = get_dataset_metadata(catalog, dataset_identifier)

    distributions = filter(
        lambda x: x.get("identifier") == distribution_identifier,
        dataset["distribution"])

    if len(distributions) > 1:
        raise ce.DistributionIdRepetitionError(
            distribution_identifier, distributions)
    elif len(distributions) < 1:
        raise ce.DistributionIdNonExistentError(distribution_identifier)

    return distributions[0]


def get_field_metadata(catalog, distribution_identifier, field_id=None,
                       field_title=None, dataset_identifier=None):

    distribution = get_distribution_metadata(catalog, distribution_identifier,
                                             dataset_identifier)

    assert field_id or field_title, "Se necesita el titulo o id del campo."

    if field_id:
        fields = filter(lambda x: x.get("id") == field_id,
                        distribution["field"])

        if len(fields) > 1:
            raise ce.FieldIdRepetitionError(field_id, fields)
        elif len(fields) < 1:
            raise ce.FieldIdNonExistentError(field_id)

    else:
        fields = filter(lambda x: x.get("title") == field_title,
                        distribution["field"])
        msg = "Hay mas de 1 field con titulo {}: {}".format(field_title,
                                                            fields)

        if len(fields) > 1:
            raise ce.FieldTitleRepetitionError(field_title, fields)
        elif len(fields) < 1:
            raise ce.FieldTitleNonExistentError(field_title)

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

    # hoja de la Distribucion
    worksheets = list(df_distrib.distribution_iedFileSheet.unique())
    msg = "Distribucion {} en mas de una hoja {}".format(
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
        df_distrib.field_title == "indice_tiempo"][
        "field_identifierCell"].iloc[0]

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

    try:
        params["time_composed"] = True
        dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet)
    except TimeIsNotComposed:
        params["time_composed"] = False
        dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet)

    return dfs


def scrape_distribution(xl, etl_params, catalog, distribution_identifier):

    distribution_params = gen_distribution_params(
        etl_params, catalog, distribution_identifier)

    return scrape_dataframe(xl, **distribution_params)


def scrape_dataset(xl, etl_params, catalog, dataset_identifier, datasets_dir,
                   debug_mode=False, replace=True):

    res = {
        "dataset_status": None,
        "distributions_ok": [],
        "distributions_error": [],
    }

    dataset_meta = get_dataset_metadata(catalog, dataset_identifier)

    if dataset_meta:
        dataset_dir = os.path.join(DATASETS_DIR, dataset_identifier)
        if not os.path.isdir(dataset_dir):
            os.mkdir(dataset_dir)
        res["dataset_status"] = "OK"
    else:
        res["dataset_status"] = "ERROR: metadata"
        return res

    # filtro los parametros para un dataset en particular
    dataset_params = etl_params[etl_params.apply(
        lambda x: x[
            "distribution_identifier"].split(".")[0] == dataset_identifier,
        axis=1)]
    distribution_ids = dataset_params.distribution_identifier.unique()

    # creo c/u de las distribuciones del dataset
    for distribution_identifier in distribution_ids:
        msg = "Distribución {}: {} ({})"
        try:
            dist_path = os.path.join(
                dataset_dir, "{}.csv".format(distribution_identifier))

            # chequea si ante la existencia del archivo hay que reemplazarlo o
            # saltearlo
            if not os.path.exists(dist_path) or replace:
                status = "Replaced" if os.path.exists(dist_path) else "Created"
                distribution = scrape_distribution(
                    xl, etl_params, catalog, distribution_identifier)
                distribution.to_csv(
                    dist_path, encoding="utf-8-sig",
                    index_label="indice_tiempo")
            else:
                status = "Skipped"

            res["distributions_ok"].append((distribution_identifier, status))
            print(msg.format(distribution_identifier, "OK", status))

        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            res["distributions_error"].append(
                (distribution_identifier, repr(e).encode("utf8")))
            print(msg.format(distribution_identifier,
                             "ERROR", repr(e).encode("utf8")))
            if debug_mode:
                raise
            res["dataset_status"] = "ERROR: scraping"

    return res


def scrape_file(ied_xlsx_path, etl_params, catalog, datasets_dir,
                replace=True):
    xl = XlSeries(ied_xlsx_path)
    ied_xlsx_filename = os.path.basename(ied_xlsx_path)

    # filtro los parametros para un excel en particular
    ied_xl_params = etl_params[etl_params.apply(
        lambda x: os.path.basename(
            x["distribution_iedFileURL"]) == ied_xlsx_filename, axis=1)]
    dataset_ids = ied_xl_params.distribution_identifier.apply(
        lambda x: x.split(".")[0]).unique()

    report_datasets = []
    report_distributions = []
    for dataset_identifier in dataset_ids:
        result = scrape_dataset(
            xl, etl_params, catalog, dataset_identifier, datasets_dir,
            replace=replace
        )

        report_datasets.append({
            "dataset_identifier": dataset_identifier,
            "dataset_status": result["dataset_status"],
            "distribution_iedFileURL": ied_xlsx_filename
        })

        distribution_result = {
            "dataset_identifier": dataset_identifier,
            "distribution_iedFileURL": ied_xlsx_filename
        }

        for distribution_id, distribution_notes in result["distributions_ok"]:
            distribution_result["distribution_identifier"] = distribution_id
            distribution_result["distribution_status"] = "OK"
            distribution_result["distribution_notes"] = distribution_notes
            report_distributions.append(distribution_result)

        for distribution_id, distribution_notes in result[
                "distributions_error"]:
            distribution_result["distribution_identifier"] = distribution_id
            distribution_result["distribution_status"] = "ERROR"
            distribution_result["distribution_notes"] = distribution_notes
            report_distributions.append(distribution_result)

    return pd.DataFrame(report_datasets), pd.DataFrame(report_distributions)


def main(catalog_json_path, etl_params_path, ied_data_dir, datasets_dir):

    catalog = pydatajson.readers.read_catalog(catalog_json_path)

    etl_params = pd.read_csv(etl_params_path,
                             dtype={"distribution_identifier": "str"})

    # compone los paths a los excels de ied
    ied_xlsx_filenames = etl_params.distribution_iedFileURL.apply(
        lambda x: os.path.basename(x)
    ).unique()
    ied_xlsx_paths = [os.path.join(ied_data_dir, filename)
                      for filename in ied_xlsx_filenames]

    # scrapea cada excel de ied y genera los reportes
    all_report_datasets = []
    all_report_distributions = []
    for ied_xlsx_path in ied_xlsx_paths:
        report_datasets, report_distributions = scrape_file(
            ied_xlsx_path, etl_params, catalog, datasets_dir, replace=False)
        all_report_datasets.append(report_datasets)
        all_report_distributions.append(report_distributions)

    # concatena todos los reportes
    complete_report_datasets = pd.concat(all_report_datasets)
    complete_report_distributions = pd.concat(all_report_distributions)

    # guarda el reporte de datasets en EXCEL
    cols_rep_dataset = [
        "distribution_iedFileURL", "dataset_identifier", "dataset_status"
    ]
    complete_report_datasets[cols_rep_dataset].to_excel(
        os.path.join(REPORTES_DIR, "reporte-datasets-scraping.xlsx"),
        encoding="utf-8", index=False)

    # guarda el reporte de distribuciones en EXCEL
    cols_rep_distribution = [
        "distribution_iedFileURL", "dataset_identifier",
        "distribution_identifier", "distribution_status", "distribution_notes"
    ]
    complete_report_distributions[cols_rep_distribution].to_excel(
        os.path.join(REPORTES_DIR, "reporte-distributions-scraping.xlsx"),
        encoding="utf-8", index=False)

    # imprime resultados a la terminal
    print(complete_report_distributions.groupby(
        "distribution_status")[["distribution_identifier"]].count())
    hours = round((NOW - arrow.now()).total_seconds() / 60.0 / 60.0)
    print("Scraping completado en {} horas".format(hours))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
