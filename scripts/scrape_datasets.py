#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scrapea datasets a partir de un catálogo y parámetros para XlSeries
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import yaml
import sys
import pandas as pd
import arrow
import shutil
import traceback
from openpyxl import load_workbook
import logging
from copy import deepcopy
from urlparse import urljoin
from pprint import pprint

from pydatajson import DataJson
import pydatajson.readers as readers
import pydatajson.writers as writers
from pydatajson.helpers import title_to_name
from xlseries.strategies.clean.parse_time import TimeIsNotComposed
from xlseries import XlSeries
from series_tiempo_ar.validations import validate_distribution_scraping
from series_tiempo_ar.validations import validate_distribution

import helpers
from helpers import get_logger
import custom_exceptions as ce
from paths import LOGS_DIR, REPORTES_DIR, CONFIG_SERVER_PATH
from paths import get_distribution_path, CATALOGS_DIR_INPUT
from generate_catalog import write_json_catalog

sys.path.insert(0, os.path.abspath(".."))

PRESERVE_WB_OBJ = False

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
    "missing_value": [
        None, "", " ", "-", "--", "---", ".", "...", "/", "///",
        "s.d.", "s.d", "s/d",
        "n,d,", "n,d", "n.d.", "n.d", "n/d",
        "s", "x"
    ],
    'time_alignment': 0,
    'time_multicolumn': False,
    "headers_coord": None,
    "data_starts": None,
    "frequency": None,
    "time_header_coord": None,
    # 'time_composed': True
}


def gen_distribution_params(etl_params, catalog, distribution_identifier):
    df_distrib = etl_params[
        etl_params.distribution_identifier == distribution_identifier
    ]
    df_fields = df_distrib[etl_params.field_title != "indice_tiempo"]

    num_series = len(df_fields)
    params = {}

    # hoja de la Distribucion
    worksheets = list(df_distrib.distribution_scrapingFileSheet.unique())
    msg = "Distribucion {} en mas de una hoja {}".format(
        distribution_identifier, worksheets)
    assert len(worksheets) == 1, msg
    params["worksheet"] = worksheets[0]

    # coordenadas de los headers de las series
    params["headers_coord"] = list(df_fields.field_scrapingIdentifierCell)

    # coordenadas de los headers de las series
    params["headers_value"] = list(df_fields.field_id)

    # fila donde empiezan los datos
    params["data_starts"] = map(helpers.row_from_cell_coord,
                                df_fields.field_scrapingDataStartCell)

    # frecuencia de las series
    field = catalog.get_field(distribution_identifier=distribution_identifier,
                              title="indice_tiempo")
    params["frequency"] = helpers.freq_iso_to_xlseries(
        field["specialTypeDetail"])

    # coordenadas del header del indice de tiempo
    params["time_header_coord"] = df_distrib[
        df_distrib.field_title == "indice_tiempo"][
        "field_scrapingIdentifierCell"].iloc[0]

    # nombres de las series
    params["series_names"] = list(df_fields.field_title)

    return params


def scrape_dataframe(xl, worksheet, headers_coord, headers_value, data_starts,
                     frequency, time_header_coord, series_names):
    params = deepcopy(XLSERIES_PARAMS)
    params["headers_coord"] = headers_coord
    params["data_starts"] = data_starts
    params["frequency"] = frequency
    params["time_header_coord"] = time_header_coord
    params["series_names"] = series_names

    try:
        params["time_composed"] = True
        dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet,
                                 preserve_wb_obj=PRESERVE_WB_OBJ)
    except TimeIsNotComposed:
        params["time_composed"] = False
        dfs = xl.get_data_frames(deepcopy(params), ws_name=worksheet,
                                 preserve_wb_obj=PRESERVE_WB_OBJ)

    return dfs


def scrape_distribution(xl, etl_params, catalog, distribution_identifier):

    distribution_params = gen_distribution_params(
        etl_params, catalog, distribution_identifier)
    distrib_meta = catalog.get_distribution(distribution_identifier)
    dataset_meta = catalog.get_dataset(distribution_identifier.split(".")[0])

    df = scrape_dataframe(xl, **distribution_params)

    if isinstance(df, list):
        df = pd.concat(df, axis=1)

    # VALIDACIONES
    worksheet = distribution_params["worksheet"]
    headers_coord = distribution_params["headers_coord"]
    headers_value = distribution_params["headers_value"]

    validate_distribution_scraping(xl, worksheet, headers_coord, headers_value)
    validate_distribution(df, catalog, dataset_meta, distrib_meta,
                          distribution_identifier)

    return df


def analyze_distribution(catalog, distribution_identifier):

    distrib_meta = catalog.get_distribution(distribution_identifier)
    dataset_meta = catalog.get_dataset(distribution_identifier.split(".")[0])

    distribution_path = get_distribution_path(
        "sspm", dataset_meta["identifier"], distribution_identifier,
        CATALOGS_DIR_INPUT)
    # print("leyendo distribucion {} en {}".format(
    #     distribution_identifier, distribution_path))

    time_index = "indice_tiempo"
    df = pd.read_csv(
        distribution_path, index_col=time_index,
        parse_dates=[time_index],
        date_parser=lambda x: arrow.get(x, "YYYY-MM-DD").datetime
        # encoding="utf-8"
    )

    validate_distribution(df, catalog, dataset_meta, distrib_meta,
                          distribution_identifier)

    return distribution_path, df


def get_distribution_url(dist_path, config_server_path=CONFIG_SERVER_PATH):

    with open(config_server_path, 'r') as f:
        server_params = yaml.load(f)

    base_url = server_params["host"]

    return urljoin(
        base_url,
        os.path.join("catalog", dist_path.split("catalog/")[1])
    )


def scrape_dataset(xl, etl_params, catalog, dataset_identifier, datasets_dir,
                   debug_mode=False, replace=True,
                   config_server_path=CONFIG_SERVER_PATH,
                   debug_distribution_ids=None, catalog_id=None):

    res = {
        "dataset_status": None,
        "distributions_ok": [],
        "distributions_error": [],
    }

    dataset_meta = catalog.get_dataset(dataset_identifier)

    if dataset_meta:
        dataset_dir = os.path.join(datasets_dir, dataset_identifier)
        helpers.ensure_dir_exists(dataset_dir)
        res["dataset_status"] = "OK"
    else:
        res["dataset_status"] = "ERROR: metadata"
        return res

    # filtro los parametros para un dataset en particular
    distribution_ids_all = [
        distribution["identifier"]
        for distribution in dataset_meta["distribution"]
    ]
    dataset_params = etl_params[etl_params.apply(
        lambda x: x["distribution_identifier"] in distribution_ids_all,
        axis=1)]
    distribution_ids = dataset_params.distribution_identifier.unique()

    # si está en debug mode, se puede especificar sólo algunos ids
    if debug_mode and debug_distribution_ids:
        distribution_ids = [
            distribution_id for distribution_id in distribution_ids
            if distribution_id in debug_distribution_ids
        ]

    # creo c/u de las distribuciones del dataset
    for distribution_identifier in distribution_ids:
        msg = "Distribución {}: {} ({})"
        try:
            distrib_meta = catalog.get_distribution(distribution_identifier)
            distribution_name = title_to_name(distrib_meta["title"])
            dist_download_dir = os.path.join(
                dataset_dir, "distribution", distribution_identifier,
                "download"
            )
            dist_path = os.path.join(dist_download_dir,
                                     "{}.csv".format(distribution_name))
            dist_url = get_distribution_url(dist_path, config_server_path)
            # print("esta es la URL QUE VA AL CATALOGO", dist_url)
            distrib_meta["downloadURL"] = dist_url

            # chequea si ante la existencia del archivo hay que reemplazarlo o
            # saltearlo
            if not os.path.exists(dist_path) or replace:
                status = "Replaced" if os.path.exists(dist_path) else "Created"
                distribution = scrape_distribution(
                    xl, etl_params, catalog, distribution_identifier)

                if isinstance(distribution, list):
                    distribution_complete = pd.concat(distribution)
                else:
                    distribution_complete = distribution

                helpers.remove_other_files(os.path.dirname(dist_path))
                distribution_complete.to_csv(
                    dist_path, encoding="utf-8",
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

            trace_string = traceback.format_exc()
            print(msg.format(
                distribution_identifier, "ERROR",
                repr(e).encode("utf8")
            ))
            print(trace_string)
            if debug_mode:
                raise
            res["dataset_status"] = "ERROR: scraping"

            # si no hay versión vieja de la distribución, elimina del catálogo
            try:
                get_distribution_path(catalog_id, dataset_identifier,
                                      distribution_identifier)
            except:
                catalog.remove_distribution(
                    distribution_identifier, dataset_identifier)

    return res


def analyze_dataset(catalog, dataset_identifier, datasets_output_dir,
                    debug_mode=False, replace=True,
                    config_server_path=CONFIG_SERVER_PATH,
                    debug_distribution_ids=None):

    res = {
        "dataset_status": None,
        "distributions_ok": [],
        "distributions_error": [],
    }

    dataset_meta = catalog.get_dataset(dataset_identifier)

    if dataset_meta:
        dataset_dir = os.path.join(datasets_output_dir, dataset_identifier)
        helpers.ensure_dir_exists(dataset_dir)
        res["dataset_status"] = "OK"
    else:
        res["dataset_status"] = "ERROR: metadata"
        return res

    distribution_ids = [distribution["identifier"]
                        for distribution in dataset_meta["distribution"]]

    # si está en debug mode, se puede especificar sólo algunos ids
    if debug_mode and debug_distribution_ids:
        distribution_ids = [
            distribution_id for distribution_id in distribution_ids
            if distribution_id in debug_distribution_ids
        ]

    # creo c/u de las distribuciones del dataset
    for distribution_identifier in distribution_ids:
        msg = "Distribución {}: {} ({})"
        try:
            distrib_meta = catalog.get_distribution(distribution_identifier)
            distribution_name = title_to_name(distrib_meta["title"])
            dist_path = os.path.join(
                dataset_dir, "distribution", distribution_identifier,
                "download", "{}.csv".format(distribution_name)
            )
            dist_url = get_distribution_url(dist_path, config_server_path)
            # print("esta es la URL QUE VA AL CATALOGO", dist_url)
            distrib_meta["downloadURL"] = dist_url

            # chequea si ante la existencia del archivo hay que reemplazarlo o
            # saltearlo
            if not os.path.exists(dist_path) or replace:
                status = "Replaced" if os.path.exists(dist_path) else "Created"
                origin_dist_path, df = analyze_distribution(
                    catalog, distribution_identifier)

                if isinstance(distribution, list):
                    distribution_complete = pd.concat(distribution)
                else:
                    distribution_complete = distribution

                helpers.ensure_dir_exists(os.path.dirname(dist_path))
                shutil.copyfile(origin_dist_path, dist_path)
            else:
                status = "Skipped"

            res["distributions_ok"].append((distribution_identifier, status))
            print(msg.format(distribution_identifier, "OK", status))

        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            res["distributions_error"].append(
                (distribution_identifier, repr(e).encode("utf8")))

            trace_string = traceback.format_exc()
            print(msg.format(
                distribution_identifier, "ERROR",
                repr(e).encode("utf8")
            ))
            print(trace_string)

            if debug_mode:
                raise
            res["dataset_status"] = "ERROR: scraping"

    return res


def scrape_file(scraping_xlsx_path, etl_params, catalog, datasets_dir,
                replace=True, debug_mode=False, debug_distribution_ids=None,
                catalog_id=None):
    xl = XlSeries(scraping_xlsx_path)
    scraping_filename = os.path.basename(scraping_xlsx_path)

    # filtro los parametros para un excel en particular
    dataset_ids = set()
    for distribution in catalog.get_distributions():
        if (
            ("scrapingFileURL" in distribution) and
                (os.path.basename(distribution[
                    "scrapingFileURL"]) == scraping_filename)
        ):
            dataset_ids.add(distribution["dataset_identifier"])

    report_datasets = []
    report_distributions = []
    for dataset_identifier in dataset_ids:
        result = scrape_dataset(
            xl, etl_params, catalog, dataset_identifier, datasets_dir,
            replace=replace, debug_mode=debug_mode,
            debug_distribution_ids=debug_distribution_ids,
            catalog_id=catalog_id
        )

        report_datasets.append({
            "dataset_identifier": dataset_identifier,
            "dataset_status": result["dataset_status"],
            "distribution_scrapingFileURL": scraping_filename
        })

        distribution_result = {
            "dataset_identifier": dataset_identifier,
            "distribution_scrapingFileURL": scraping_filename
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

        # si se eliminaron las distribuciones del dataset, se borra el dataset
        distributions = catalog.get_dataset(
            dataset_identifier).get("distribution", [])
        if len(distributions) == 0:
            print("Se elimina el dataset {}, no tiene distribuciones".format(
                dataset_identifier
            ))
            catalog.remove_dataset(dataset_identifier)

    return pd.DataFrame(report_datasets), pd.DataFrame(report_distributions)


def analyze_catalog(catalog, datasets_dir,
                    replace=True, debug_mode=False,
                    debug_distribution_ids=None):
    logger = get_logger(__name__)

    distributions_with_url = filter(
        lambda x: "downloadURL" in x and bool(x["downloadURL"]),
        catalog.get_distributions()
    )
    logger.info("{} distribuciones con `downloadURL`".format(
        len(distributions_with_url))
    )
    dataset_ids = set((
        distribution["dataset_identifier"]
        for distribution in distributions_with_url
    ))

    report_datasets = []
    report_distributions = []
    for dataset_identifier in dataset_ids:
        result = analyze_dataset(
            catalog, dataset_identifier, datasets_dir,
            replace=replace, debug_mode=debug_mode,
            debug_distribution_ids=debug_distribution_ids
        )

        report_datasets.append({
            "dataset_identifier": dataset_identifier,
            "dataset_status": result["dataset_status"],
            "distribution_scrapingFileURL": None
        })

        distribution_result = {
            "dataset_identifier": dataset_identifier,
            "distribution_scrapingFileURL": None
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


def generate_summary_message(indicators):
    """Genera asunto y mensaje del mail de reporte a partir de indicadores.

    Args:
        indicators (dict):

    Return:
        tuple: (str, str) (asunto, mensaje)
    """
    server_environment = os.environ.get("SERVER_ENVIRONMENT", "desconocido")
    subject = "[{}] Series de Tiempo ETL: {}".format(
        server_environment,
        arrow.now().format("DD/MM/YYYY HH:mm")
    )
    message = helpers.indicators_to_text(indicators)
    return subject, message


def generate_summary_indicators(report_files, report_datasets,
                                report_distributions):

    distr_ok = len(report_distributions[
        report_distributions.distribution_status == "OK"
    ])
    distr_error = len(report_distributions[
        report_distributions.distribution_status != "OK"
    ])
    indicators = {
        "Datasets": len(report_datasets),
        "Datasets (OK)": len(report_datasets[
            report_datasets.dataset_status == "OK"
        ]),
        "Datasets (ERROR)": len(report_datasets[
            report_datasets.dataset_status != "OK"
        ]),
        "Distribuciones": len(report_distributions),
        "Distribuciones (OK)": distr_ok,
        "Distribuciones (ERROR)": distr_error,
        "Distribuciones (OK %)": round(
            float(distr_ok) / (distr_ok + distr_error), 3) * 100,
    }

    if report_files is not None and len(report_files) > 0:
        indicators_files = {
            "Archivos": len(report_files),
            "Archivos (OK)": len(report_files[
                report_files.file_status == "OK"
            ]),
            "Archivos (ERROR)": len(report_files[
                report_files.file_status != "OK"
            ])
        }
        indicators.update(indicators_files)

    return indicators


def main(catalog_json_path, etl_params_path, ied_data_dir,
         datasets_dir, catalog_id,
         replace=False, debug_mode=False, debug_distribution_ids=None,
         do_scraping=True, do_distributions=True):
    server_environment = os.environ.get("SERVER_ENVIRONMENT", "desconocido")
    # en un ambiente productivo SIEMPRE reemplaza por la nueva opción

    if server_environment == "prod":
        replace = True

    catalog = DataJson(catalog_json_path)

    etl_params = pd.read_csv(etl_params_path,
                             dtype={"distribution_identifier": "str"})

    # compone los paths a los excels de ied
    ied_xlsx_filenames = etl_params.distribution_scrapingFileURL.apply(
        lambda x: os.path.basename(x)
    ).unique()
    ied_xlsx_paths = [os.path.join(ied_data_dir, filename)
                      for filename in ied_xlsx_filenames]

    # inicializa las listas que contienen los reportes
    report_files = []
    all_report_datasets = []
    all_report_distributions = []

    # PRIMERO: recorre catálogo en busca de distribuciones con series de tiempo
    if do_distributions:
        try:
            report_datasets, report_distributions = analyze_catalog(
                catalog, datasets_dir,
                replace=replace, debug_mode=debug_mode,
                debug_distribution_ids=debug_distribution_ids)
            all_report_datasets.append(report_datasets)
            all_report_distributions.append(report_distributions)

        except Exception as e:
            raise
            if isinstance(e, KeyboardInterrupt):
                raise
            if debug_mode:
                raise

    # SEGUNDO: organiza el scraping por Excel descargado
    if do_scraping:
        msg = "Archivo {}: {} ({})"
        for ied_xlsx_path in ied_xlsx_paths:
            print(ied_xlsx_path)

            try:
                report_datasets, report_distributions = scrape_file(
                    ied_xlsx_path, etl_params, catalog, datasets_dir,
                    replace=replace, debug_mode=debug_mode,
                    debug_distribution_ids=debug_distribution_ids,
                    catalog_id=catalog_id)
                all_report_datasets.append(report_datasets)
                all_report_distributions.append(report_distributions)

                report_files.append({
                    "file_name": ied_xlsx_path,
                    "file_status": "OK",
                    "file_notes": ""
                })

            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise
                report_files.append({
                    "file_name": ied_xlsx_path,
                    "file_status": "ERROR",
                    "file_notes": repr(e).encode("utf8")
                })

                trace_string = traceback.format_exc()
                print(msg.format(ied_xlsx_path, "ERROR",
                                 repr(e).encode("utf8")))
                print(trace_string)
                if debug_mode:
                    raise

    # concatena todos los reportes
    complete_report_files = pd.DataFrame(report_files)
    complete_report_datasets = pd.concat(all_report_datasets)
    complete_report_distributions = pd.concat(all_report_distributions)

    # guarda el reporte de archivos en EXCEL
    cols_rep_files = [
        "file_name", "file_status", "file_notes"
    ]
    if len(complete_report_files) > 0:
        complete_report_files[cols_rep_files].to_excel(
            os.path.join(REPORTES_DIR, "reporte-files-scraping.xlsx"),
            encoding="utf-8", index=False)

    # guarda el reporte de datasets en EXCEL
    cols_rep_dataset = [
        "distribution_scrapingFileURL", "dataset_identifier", "dataset_status"
    ]
    complete_report_datasets[cols_rep_dataset].to_excel(
        os.path.join(REPORTES_DIR, "reporte-datasets-scraping.xlsx"),
        encoding="utf-8", index=False)

    # guarda el reporte de distribuciones en EXCEL
    cols_rep_distribution = [
        "distribution_scrapingFileURL", "dataset_identifier",
        "distribution_identifier", "distribution_status", "distribution_notes"
    ]
    complete_report_distributions[cols_rep_distribution].to_excel(
        os.path.join(REPORTES_DIR, "reporte-distributions-scraping.xlsx"),
        encoding="utf-8", index=False)

    # imprime resultados a la terminal
    indicators = generate_summary_indicators(
        complete_report_files,
        complete_report_datasets,
        complete_report_distributions
    )
    subject, message = generate_summary_message(indicators)

    with open(os.path.join(REPORTES_DIR, "mail_subject.txt"), "wb") as f:
        f.write(subject)
    with open(os.path.join(REPORTES_DIR, "mail_message.txt"), "wb") as f:
        f.write(message)

    print("Escribiendo nueva version de {}".format(catalog_json_path))
    write_json_catalog(catalog, catalog_json_path)
    print(message)


if __name__ == '__main__':
    if len(sys.argv) >= 7 and sys.argv[6]:
        replace = True if sys.argv[6] == "replace" else False
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5],
         replace=replace, debug_mode=False, debug_distribution_ids=[])
