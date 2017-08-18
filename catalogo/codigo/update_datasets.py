#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Actualiza las series en un servidor webdav
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import sys
import arrow
import pydatajson
import json
from webdav import get_webdav_connection
import yaml
import os
import glob
import pysftp
import requests
import yaml
import os
from pydatajson import DataJson
import time
from bs4 import BeautifulSoup

from infra import upload_file_to_ind, do_ind_api_request
from helpers import get_logger
from data import get_time_series_dict, generate_time_series_jsons
from bs4 import BeautifulSoup

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
sys.path.insert(0, os.path.abspath(".."))


def get_distribution_ind_ids():
    query = """
        SELECT
            ct.id_catalog, ct.nombre, ds.id_distribution, ds.identificador
        FROM distribution ds
            inner join dataset dt on (dt.id_dataset = ds.id_dataset)
            inner join catalog ct on (dt.id_catalog= ct.id_catalog)
        WHERE
            ds.vigente = TRUE and
            dt.vigente = TRUE and
            ct.vigente = TRUE;
    """

    engine = create_engine(
        'postgresql://user_da:0YCbqX569a@192.168.150.61:5432/modernizacion')

    index_cols = ["nombre", "identificador"]
    df_distribs = pd.read_sql_query(
        query, engine).dropna().sort_values(index_cols)
    df = df_distribs.drop_duplicates(
        index_cols, keep="last").set_index(index_cols)
    df.sort_index(level=index_cols, ascending=[1, 0], inplace=True)

    return df


def upload_distribution_to_ind(local_path, distribution_id, config_ind_path="config/config_ind_dev.yaml"):

    with open(config_ind_path, 'r') as f:
        ind_params = yaml.load(f)

    file_name = "{}.csv".format(distribution_id)
    upload_file_to_ind(local_path, file_name=file_name, config_ind=ind_params)

    status = do_ind_api_request(distribution_id_ind=distribution_id,
                                req_type="data", config_ind=ind_params)

    return status


def upload_distributions():
    df = get_distribution_ind_ids()

    status_uploads = {}
    for dataset in catalog["dataset"]:
        for distribution in dataset["distribution"]:
            try:
                id_distribution = df.loc[catalog["title"], distribution[
                    "identifier"]]["id_distribution"]

                distribution_path = os.path.join(
                    DATASETS_DIR, dataset["identifier"], "{}.csv".format(distribution["identifier"]))

                # renamed_path = "/Users/abenassi/github/series-tiempo/catalogo/datos/datasets_ind/{}.csv".format(id_distribution)
                # shutil.copyfile(distribution_path, renamed_path)
                status = upload_distribution_to_ind(
                    distribution_path, id_distribution)

                if not status in status_uploads:
                    status_uploads[status] = []
                status_uploads[status].append({
                    "dataset_identifier": dataset["identifier"],
                    "distribution_identifier": distribution["identifier"],
                    "distribution_id_ind": id_distribution
                })
                print(dataset["identifier"], distribution[
                      "identifier"], id_distribution, status)

            except Exception as e:
                if not str(e) in status_uploads:
                    status_uploads[str(e)] = []
                status_uploads[str(e)].append({
                    "dataset_identifier": dataset["identifier"],
                    "distribution_identifier": distribution["identifier"],
                    "distribution_id_ind": id_distribution
                })
                print(dataset["identifier"], distribution[
                      "identifier"], id_distribution, e)
                continue

    return status_uploads


def main(catalog_json_path, config_ind_path):
    logger = get_logger(__name__)

    try:
        logger.info("Cargando catalogo en la Infraestructura de Datos...")
        logger.info(upload_datajson_to_ind(
            catalog_json_path, config_ind_path, logger))
        logger.info("Actualizacion finalizada.")
    except Exception as e:
        logger.error(repr(e))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
