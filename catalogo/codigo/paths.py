#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Variables globales para facilitar la navegación de la estructura del repo
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import with_statement
import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))

# directorios del repositorio
CATALOG_DIR = os.path.join("catalogo")
LOGS_DIR = os.path.join("catalogo", "logs")
DATOS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos")
DEFAULT_CATALOG_PATH = os.path.join(
    PROJECT_DIR, "catalogo", "datos", "data.json")
DUMPS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "dumps")
EXCELS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "ied")
SERIES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "series")
DATASETS_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "datasets")
DATASETS_TEST_DIR = os.path.join(
    PROJECT_DIR, "catalogo", "datos", "test", "datasets")
CATALOGS_HISTORY_DIR = os.path.join(
    PROJECT_DIR, "catalogo", "datos", "catalogos")
REPORTES_DIR = os.path.join(PROJECT_DIR, "catalogo", "datos", "reportes")
CODIGO_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo")
SCHEMAS_DIR = os.path.join(PROJECT_DIR, "catalogo", "codigo", "schemas")
