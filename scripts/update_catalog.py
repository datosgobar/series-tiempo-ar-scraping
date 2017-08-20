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


def upload_datajson_to_ind(
        local_path, config_ind_path="config/config_ind.yaml", logger=None):

    with open(config_ind_path, 'r') as f:
        ind_params = yaml.load(f)

    upload_file_to_ind(local_path, config_ind=ind_params)

    if logger:
        logger.info("Actualizando metadatos...")
        sys.stdout.flush()
    status = do_ind_api_request(req_type="metadata", config_ind=ind_params)

    return status


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
