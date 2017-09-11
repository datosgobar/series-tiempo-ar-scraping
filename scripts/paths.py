#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Variables globales para facilitar la navegación de la estructura del repo
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os
import glob

PROJECT_DIR = os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))

# directorios del repositorio
LOGS_DIR = os.path.join(PROJECT_DIR, "logs")
DATOS_DIR = os.path.join(PROJECT_DIR, "data")
CATALOGS_DIR = os.path.join(DATOS_DIR, "output", "server")
CATALOGS_DIR_INPUT = os.path.join(DATOS_DIR, "input")
TEST_CATALOGS_DIR = os.path.join(DATOS_DIR, "tests")
DUMPS_DIR = os.path.join(DATOS_DIR, "output", "dump")
CATALOG_PATH = os.path.join(
    DATOS_DIR, "output", "catalog", "sspm", "data.json")
BACKUP_CATALOG_DIR = os.path.join(DATOS_DIR, "backup", "catalog")
DUMPS_PARAMS_PATH = os.path.join(
    DATOS_DIR, "params", "dumps_params.json")
SOURCES_DIR = os.path.join(DATOS_DIR, "catalog", "sspm", "source")
SERIES_DIR = os.path.join(DATOS_DIR, "output", "series")
DATASETS_DIR = os.path.join(
    DATOS_DIR, "output", "catalog", "sspm", "dataset")
CATALOGS_HISTORY_DIR = os.path.join(DATOS_DIR, "catalog", "sspm")
REPORTES_DIR = os.path.join(DATOS_DIR, "reports")
CODIGO_DIR = os.path.join(PROJECT_DIR, "scripts")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "scripts", "schemas")
CONFIG_EMAIL_PATH = os.path.join(
    PROJECT_DIR, "scripts", "config", "config_email.yaml")
CONFIG_SERVER_PATH = os.path.join(
    PROJECT_DIR, "scripts", "config", "config_server.yaml")


def get_distribution_path(catalog_id, dataset_id, distribution_id,
                          catalogs_dir=CATALOGS_DIR):
    distribution_download_dir = os.path.join(
        catalogs_dir, "catalog", catalog_id, "dataset", dataset_id,
        "distribution", distribution_id, "download"
    )
    glob_pattern = os.path.join(distribution_download_dir, "*.csv")
    distribution_csv_files = glob.glob(glob_pattern)

    if len(distribution_csv_files) == 1:
        return distribution_csv_files[0]
    elif len(distribution_csv_files) == 0:
        raise Exception(
            "Sin archivos para la distribucion {} del dataset {}\n{}".format(
                distribution_id, dataset_id, glob_pattern))
    else:
        raise Exception(
            "{} archivos para la distribucion {} del dataset {}\n{}".format(
                len(distribution_csv_files), distribution_id,
                dataset_id, glob_pattern)
        )


def get_catalogs_path(catalogs_dir=CATALOGS_DIR):
    return glob.glob(os.path.join(catalogs_dir, "catalog", "*", "*.json"))


def get_catalog_path(catalog_id, catalogs_dir=CATALOGS_DIR):
    return os.path.join(catalogs_dir, "catalog", catalog_id, "data.json")


def get_catalog_ids(catalogs_dir=CATALOGS_DIR):
    return [os.path.basename(os.path.dirname(catalog_path))
            for catalog_path in get_catalogs_path(catalogs_dir)]
