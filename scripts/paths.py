#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Variables globales para facilitar la navegación de la estructura del repo
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os

PROJECT_DIR = os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))

# directorios del repositorio
LOGS_DIR = os.path.join(PROJECT_DIR, "logs")
DATOS_DIR = os.path.join(PROJECT_DIR, "data")
DUMPS_DIR = os.path.join(DATOS_DIR, "output", "dump")
CATALOG_PATH = os.path.join(
    DATOS_DIR, "output", "catalog", "sspm", "data.json")
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
